use std::str::FromStr;

use base64::Engine;
use chacha20poly1305::{
  aead::{generic_array::typenum::Unsigned, OsRng},
  AeadCore, AeadInPlace, ChaCha20Poly1305, KeyInit, XChaCha20Poly1305, XNonce,
};
use rand::Rng;

pub struct CryptoRootKey {
  inner: CryptoRootKeyInner,
  subkey_algorithm: SubkeyAlgorithm,
}

enum CryptoRootKeyInner {
  XChaCha20Poly1305(XChaCha20Poly1305),
}

#[derive(Clone, Copy, Debug)]
pub enum SubkeyAlgorithm {
  ChaCha20Poly1305,
  Aes256Gcm,
}

pub struct CryptoSubKey(CryptoSubKeyInner);

enum CryptoSubKeyInner {
  Unencrypted,
  ChaCha20Poly1305(ChaCha20Poly1305),
  Aes256Gcm([u8; 32]),
}

impl CryptoRootKey {
  pub fn new(raw: &str, subkey_algorithm: SubkeyAlgorithm) -> anyhow::Result<Self> {
    let (algorithm, key) = raw
      .split_once(':')
      .ok_or_else(|| anyhow::anyhow!("key format is invalid"))?;

    // do not propagate base64 decode error
    let key = base64::engine::general_purpose::STANDARD
      .decode(key)
      .map_err(|_| anyhow::anyhow!("failed to decode key"))?;

    let inner = match algorithm {
      "xchacha20poly1305" => {
        CryptoRootKeyInner::XChaCha20Poly1305(XChaCha20Poly1305::new_from_slice(&key)?)
      }
      _ => {
        // do not propagate `algorithm` string - may contain sensitive data
        anyhow::bail!("unknown algorithm");
      }
    };

    Ok(Self {
      inner,
      subkey_algorithm,
    })
  }

  pub fn generate_subkey(&self) -> (CryptoSubKey, Vec<u8>) {
    let subkey_str = match self.subkey_algorithm {
      SubkeyAlgorithm::ChaCha20Poly1305 => {
        format!(
          "chacha20poly1305:{}",
          base64::engine::general_purpose::STANDARD.encode(ChaCha20Poly1305::generate_key(OsRng))
        )
      }
      SubkeyAlgorithm::Aes256Gcm => {
        format!(
          "aes256gcm:{}",
          base64::engine::general_purpose::STANDARD.encode(rand::thread_rng().gen::<[u8; 32]>()),
        )
      }
    };
    let subkey = CryptoSubKey::parse(&subkey_str).unwrap();
    (
      subkey,
      self.encrypt_with_random_nonce(subkey_str.into()).unwrap(),
    )
  }

  pub fn decode_subkey(&self, subkey: Vec<u8>) -> Option<CryptoSubKey> {
    let subkey = self.decrypt_with_random_nonce(subkey)?;
    let subkey = std::str::from_utf8(&subkey).ok()?;
    let subkey = CryptoSubKey::parse(subkey).ok()?;
    Some(subkey)
  }

  fn encrypt_with_random_nonce(&self, plaintext: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    match &self.inner {
      CryptoRootKeyInner::XChaCha20Poly1305(key) => {
        const TAG_SIZE: usize = <XChaCha20Poly1305 as AeadCore>::TagSize::USIZE;
        const NONCE_SIZE: usize = <XChaCha20Poly1305 as AeadCore>::NonceSize::USIZE;
        let nonce = rand::thread_rng().gen::<[u8; NONCE_SIZE]>();
        let plaintext_len = plaintext.len();
        let mut output = plaintext;
        output.reserve(TAG_SIZE + NONCE_SIZE);
        key
          .encrypt_in_place(nonce[..].try_into().unwrap(), &[], &mut output)
          .map_err(|_| anyhow::anyhow!("encrypt failed"))?;
        assert_eq!(output.len(), plaintext_len + TAG_SIZE);
        output.extend_from_slice(&nonce);
        Ok(output)
      }
    }
  }

  fn decrypt_with_random_nonce(&self, ciphertext: Vec<u8>) -> Option<Vec<u8>> {
    match &self.inner {
      CryptoRootKeyInner::XChaCha20Poly1305(key) => {
        const TAG_SIZE: usize = <XChaCha20Poly1305 as AeadCore>::TagSize::USIZE;
        const NONCE_SIZE: usize = <XChaCha20Poly1305 as AeadCore>::NonceSize::USIZE;
        if ciphertext.len() < TAG_SIZE + NONCE_SIZE {
          return None;
        }
        let nonce = *XNonce::from_slice(&ciphertext[ciphertext.len() - NONCE_SIZE..]);
        let ciphertext_len = ciphertext.len();
        let mut output = ciphertext;
        output.truncate(ciphertext_len - NONCE_SIZE);
        key.decrypt_in_place(&nonce, &[], &mut output).ok()?;
        Some(output)
      }
    }
  }
}

impl CryptoSubKey {
  pub fn unencrypted() -> Self {
    Self(CryptoSubKeyInner::Unencrypted)
  }

  pub fn overhead_bytes(&self) -> usize {
    match &self.0 {
      CryptoSubKeyInner::Unencrypted => 0,
      CryptoSubKeyInner::ChaCha20Poly1305(_) => <ChaCha20Poly1305 as AeadCore>::TagSize::USIZE,
      CryptoSubKeyInner::Aes256Gcm(_) => 16,
    }
  }

  fn parse(raw: &str) -> anyhow::Result<Self> {
    let (algorithm, key) = raw
      .split_once(':')
      .ok_or_else(|| anyhow::anyhow!("key format is invalid"))?;

    // do not propagate base64 decode error
    let key = base64::engine::general_purpose::STANDARD
      .decode(key)
      .map_err(|_| anyhow::anyhow!("failed to decode key"))?;

    match algorithm {
      "chacha20poly1305" => Ok(Self(CryptoSubKeyInner::ChaCha20Poly1305(
        ChaCha20Poly1305::new_from_slice(&key)?,
      ))),
      "aes256gcm" => Ok(Self(CryptoSubKeyInner::Aes256Gcm(<[u8; 32]>::try_from(
        &key[..],
      )?))),
      "unencrypted" if key.is_empty() => Ok(Self(CryptoSubKeyInner::Unencrypted)),
      _ => {
        // do not propagate `algorithm` string - may contain sensitive data
        Err(anyhow::anyhow!("unknown algorithm"))
      }
    }
  }

  pub fn encrypt_with_u64_le_nonce(&self, plaintext: Vec<u8>, nonce: u64) -> Vec<u8> {
    match &self.0 {
      CryptoSubKeyInner::Unencrypted => plaintext,
      CryptoSubKeyInner::ChaCha20Poly1305(key) => {
        const TAG_SIZE: usize = <ChaCha20Poly1305 as AeadCore>::TagSize::USIZE;
        const NONCE_SIZE: usize = <ChaCha20Poly1305 as AeadCore>::NonceSize::USIZE;

        let mut nonce_bytes = [0u8; NONCE_SIZE];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());
        let mut output = plaintext;
        output.reserve(TAG_SIZE);
        key
          .encrypt_in_place(nonce_bytes[..].try_into().unwrap(), &[], &mut output)
          .unwrap();
        output
      }
      CryptoSubKeyInner::Aes256Gcm(key) => {
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());
        let mut tag = [0u8; 16];
        let mut output = boring::symm::encrypt_aead(
          boring::symm::Cipher::aes_256_gcm(),
          key,
          Some(&nonce_bytes),
          &[],
          &plaintext,
          &mut tag,
        )
        .unwrap();
        output.extend_from_slice(&tag);
        output
      }
    }
  }

  pub fn decrypt_with_u64_le_nonce(
    &self,
    ciphertext: Vec<u8>,
    nonce: u64,
  ) -> anyhow::Result<Vec<u8>> {
    match &self.0 {
      CryptoSubKeyInner::Unencrypted => Ok(ciphertext),
      CryptoSubKeyInner::ChaCha20Poly1305(key) => {
        const NONCE_SIZE: usize = <ChaCha20Poly1305 as AeadCore>::NonceSize::USIZE;

        let mut nonce_bytes = [0u8; NONCE_SIZE];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());
        let mut output = ciphertext;
        key
          .decrypt_in_place(nonce_bytes[..].try_into().unwrap(), &[], &mut output)
          .map_err(|_| anyhow::anyhow!("decrypt failed"))?;
        Ok(output)
      }
      CryptoSubKeyInner::Aes256Gcm(key) => {
        if ciphertext.len() < 16 {
          anyhow::bail!("ciphertext too short");
        }

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());
        let out = boring::symm::decrypt_aead(
          boring::symm::Cipher::aes_256_gcm(),
          key,
          Some(&nonce_bytes),
          &[],
          &ciphertext[..ciphertext.len() - 16],
          &ciphertext[ciphertext.len() - 16..],
        )?;
        Ok(out)
      }
    }
  }
}

impl FromStr for SubkeyAlgorithm {
  type Err = ();

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "chacha20poly1305" => Ok(Self::ChaCha20Poly1305),
      "aes256gcm" => Ok(Self::Aes256Gcm),
      _ => Err(()),
    }
  }
}

#[cfg(test)]
mod tests {
  use aes_gcm::{aead::Aead, Aes256Gcm, KeyInit};
  use base64::Engine;
  use chacha20poly1305::XChaCha20Poly1305;
  use rand::Rng;

  use super::{CryptoRootKey, CryptoSubKeyInner, SubkeyAlgorithm};

  #[test]
  fn subkey_sealing_aes256gcm() {
    let raw_root_key = rand::thread_rng().gen::<[u8; 32]>();
    let key = CryptoRootKey::new(
      &format!(
        "xchacha20poly1305:{}",
        base64::engine::general_purpose::STANDARD.encode(raw_root_key)
      ),
      SubkeyAlgorithm::Aes256Gcm,
    )
    .unwrap();

    let (subkey, encrypted_subkey) = key.generate_subkey();

    // Check subkey encryption correctness
    {
      let our_decrypted_subkey = XChaCha20Poly1305::new_from_slice(&raw_root_key)
        .unwrap()
        .decrypt(
          encrypted_subkey[encrypted_subkey.len() - 24..]
            .try_into()
            .unwrap(),
          &encrypted_subkey[..encrypted_subkey.len() - 24],
        )
        .unwrap();
      let their_decrypted_subkey =
        Vec::from(match key.decode_subkey(encrypted_subkey).unwrap().0 {
          CryptoSubKeyInner::Aes256Gcm(x) => format!(
            "aes256gcm:{}",
            base64::engine::general_purpose::STANDARD.encode(x)
          ),
          _ => unreachable!(),
        });
      let real_subkey = Vec::from(match subkey.0 {
        CryptoSubKeyInner::Aes256Gcm(x) => format!(
          "aes256gcm:{}",
          base64::engine::general_purpose::STANDARD.encode(x)
        ),
        _ => unreachable!(),
      });
      assert_eq!(their_decrypted_subkey[..], our_decrypted_subkey[..]);
      assert_eq!(our_decrypted_subkey[..], real_subkey[..]);
    }

    let data = b"hello world";
    let nonce = 42u64;

    let raw_subkey = match subkey.0 {
      super::CryptoSubKeyInner::Aes256Gcm(x) => x,
      _ => unreachable!(),
    };

    // Use a different AES implementation (`aes-gcm`) to validate data encryption/decryption correctness
    {
      let encrypted = subkey.encrypt_with_u64_le_nonce(data.to_vec(), nonce);

      let decrypted = Aes256Gcm::new_from_slice(&raw_subkey)
        .unwrap()
        .decrypt(
          [&nonce.to_le_bytes()[..], &[0u8; 4]].concat()[..]
            .try_into()
            .unwrap(),
          &encrypted[..],
        )
        .unwrap();
      assert_eq!(decrypted[..], data[..]);
    }

    {
      let encrypted = Aes256Gcm::new_from_slice(&raw_subkey)
        .unwrap()
        .encrypt(
          [&nonce.to_le_bytes()[..], &[0u8; 4]].concat()[..]
            .try_into()
            .unwrap(),
          &data[..],
        )
        .unwrap();
      let decrypted = subkey.decrypt_with_u64_le_nonce(encrypted, nonce).unwrap();
      assert_eq!(decrypted[..], data[..]);
    }
  }
}
