use std::ops::Deref;

use bytes::Bytes;
use mvps_proto::blob::BlobHeader;
use prost::Message;

use crate::{
  blob_crypto::{CryptoRootKey, CryptoSubKey},
  consts::BLOB_MAGIC,
};

pub struct BlobHeaderWriter {
  output: BlobHeader,
  last_page: LastPage,
  subkey: CryptoSubKey,
}

struct LastPage {
  id: Option<u64>,
  compressed_size: u64,
}

#[derive(Default, Clone, Debug)]
pub struct PageInfoInHeader {
  pub id: u64,
  pub compressed_size: u64,
}

#[derive(Clone, Debug)]
pub struct BlobHeaderWriterOptions {
  pub metadata: String,
}

pub enum BlobEncryptionAlgorithm {}

impl BlobHeaderWriter {
  pub fn new(options: BlobHeaderWriterOptions, root_key: Option<&CryptoRootKey>) -> Self {
    let mut v2_encrypted_data_encryption_key = Bytes::new();

    let subkey = if let Some(root_key) = root_key {
      let (subkey, encrypted) = root_key.generate_subkey();
      v2_encrypted_data_encryption_key = encrypted.into();
      subkey
    } else {
      CryptoSubKey::unencrypted()
    };

    Self {
      output: BlobHeader {
        version: 2,
        page_ids_delta_encoded: vec![],
        page_compressed_sizes_delta_encoded: vec![],
        metadata: options.metadata,
        v2_encrypted_data_encryption_key,
      },
      last_page: LastPage {
        id: None,
        compressed_size: 0,
      },
      subkey,
    }
  }

  pub fn subkey(&self) -> &CryptoSubKey {
    &self.subkey
  }

  pub fn encode(self) -> (Bytes, CryptoSubKey) {
    let header_encoded = zstd::encode_all(&self.output.encode_to_vec()[..], 0).unwrap();
    let bytes = Bytes::from(
      [
        BLOB_MAGIC,
        &(header_encoded.len() as u32).to_le_bytes(),
        &header_encoded,
      ]
      .concat(),
    );

    (bytes, self.subkey)
  }

  pub fn add_page(&mut self, info: PageInfoInHeader) -> anyhow::Result<()> {
    if let Some(last_page_id) = self.last_page.id {
      if info.id <= last_page_id {
        anyhow::bail!("page info out of order");
      }
    }

    self
      .output
      .page_ids_delta_encoded
      .push((info.id as i64).wrapping_sub((self.last_page.id.unwrap_or_default()) as i64));
    self
      .output
      .page_compressed_sizes_delta_encoded
      .push((info.compressed_size as i64).wrapping_sub(self.last_page.compressed_size as i64));

    self.last_page = LastPage {
      id: Some(info.id),
      compressed_size: info.compressed_size,
    };
    Ok(())
  }
}

impl Deref for BlobHeaderWriter {
  type Target = BlobHeader;

  fn deref(&self) -> &Self::Target {
    &self.output
  }
}
