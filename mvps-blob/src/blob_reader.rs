use std::{collections::BTreeMap, pin::Pin, rc::Rc, sync::Arc};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use mvps_proto::blob::{BlobHeader, BlobPage, BlobPageCompressionMethod};
use tokio::task::spawn_blocking;

use crate::{
  blob_crypto::{CryptoRootKey, CryptoSubKey},
  consts::BLOB_MAGIC,
  interfaces::RemoteBlob,
};

#[derive(Clone)]
pub struct BlobReader {
  backend: Rc<dyn RemoteBlob>,
  subkey: Arc<CryptoSubKey>,
  body_offset: u64,
  page_index: BTreeMap<u64, PageMetadata>,
  metadata: String,
}

#[derive(Clone, Debug)]
pub struct PageMetadata {
  pub offset_in_blob: u64,
  pub compressed_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecompressedPage {
  pub data: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PagePresence {
  Present(DecompressedPage),
  Tombstone,
  NotPresent,
}

impl BlobReader {
  pub async fn open(
    backend: Rc<dyn RemoteBlob>,
    decryption_keys: &[CryptoRootKey],
  ) -> anyhow::Result<Self> {
    let magic_and_len = backend.read_range(0..16).await?;
    if magic_and_len[0..BLOB_MAGIC.len()] != *BLOB_MAGIC {
      anyhow::bail!("magic mismatch");
    }

    let compressed_header_len =
      u32::from_le_bytes(magic_and_len[12..16].try_into().unwrap()) as u64;
    let compressed_header = backend.read_range(16..16 + compressed_header_len).await?;
    let header = Bytes::from(zstd::decode_all(&compressed_header[..])?);
    let header: BlobHeader = prost::Message::decode(header)?;

    if header.version > 2 {
      anyhow::bail!("unsupported version in header: {}", header.version);
    }

    // Decrypt subkey
    let subkey = if !header.v2_encrypted_data_encryption_key.is_empty() {
      let mut subkey: Option<CryptoSubKey> = None;
      for key in decryption_keys {
        if let Some(this) = key.decode_subkey(header.v2_encrypted_data_encryption_key.to_vec()) {
          subkey = Some(this);
          break;
        }
      }
      subkey.ok_or_else(|| anyhow::anyhow!("no provided decryption keys can decrypt subkey"))?
    } else {
      CryptoSubKey::unencrypted()
    };

    #[derive(Default, Copy, Clone)]
    struct State {
      id: i64,
      offset_in_blob: i64,
      compressed_size: i64,
    }

    let page_index: BTreeMap<u64, PageMetadata> = header
      .page_ids_delta_encoded
      .iter()
      .copied()
      .zip(header.page_compressed_sizes_delta_encoded.iter().copied())
      .scan(
        State {
          offset_in_blob: 16 + compressed_header_len as i64,
          ..Default::default()
        },
        |state, curr| {
          state.id += curr.0;
          state.compressed_size += curr.1;

          let State {
            id,
            offset_in_blob,
            compressed_size,
          } = *state;
          state.offset_in_blob += compressed_size;

          let metadata = PageMetadata {
            offset_in_blob: offset_in_blob as u64,
            compressed_size: compressed_size as u64,
          };
          Some((id as u64, metadata))
        },
      )
      .collect();

    Ok(Self {
      backend,
      subkey: Arc::new(subkey),
      body_offset: 16 + compressed_header_len,
      page_index,
      metadata: header.metadata,
    })
  }

  pub fn backend(&self) -> &Rc<dyn RemoteBlob> {
    &self.backend
  }

  pub fn body_offset(&self) -> u64 {
    self.body_offset
  }

  pub fn page_index(&self) -> &BTreeMap<u64, PageMetadata> {
    &self.page_index
  }

  pub fn metadata(&self) -> &str {
    &self.metadata
  }

  pub fn subkey(&self) -> &CryptoSubKey {
    &self.subkey
  }

  pub fn size(&self) -> u64 {
    self
      .page_index
      .last_key_value()
      .map(|(_, page_metadata)| page_metadata.offset_in_blob + page_metadata.compressed_size)
      .unwrap_or(self.body_offset)
  }

  pub async fn read_page(&self, id: u64) -> anyhow::Result<PagePresence> {
    match self.page_index.get(&id) {
      Some(metadata) if metadata.compressed_size == 0 => Ok(PagePresence::Tombstone),
      Some(metadata) => {
        let encrypted_page = self
          .backend
          .read_range(metadata.offset_in_blob..metadata.offset_in_blob + metadata.compressed_size)
          .await?;

        let compressed_page = self.subkey.decrypt_with_u64_le_nonce(encrypted_page, id)?;
        let compressed_page: BlobPage = prost::Message::decode(&compressed_page[..])?;
        let decompressed_data = match compressed_page.compression() {
          BlobPageCompressionMethod::BpcmUnspecified => anyhow::bail!("unspecified compression"),
          BlobPageCompressionMethod::BpcmNone => compressed_page.data.clone(),
          BlobPageCompressionMethod::BpcmZstd => {
            Bytes::from(spawn_blocking(move || zstd::decode_all(&compressed_page.data[..])).await??)
          }
        };

        Ok(PagePresence::Present(DecompressedPage {
          data: decompressed_data,
        }))
      }
      None => Ok(PagePresence::NotPresent),
    }
  }

  pub async fn stream_pages(
    &self,
  ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<(u64, Bytes)>> + Send + 'static>>>
  {
    let backend_stream = self
      .backend
      .clone()
      .stream_raw_chunks(
        self.body_offset,
        self
          .page_index
          .iter()
          .map(|x| x.1.compressed_size)
          .collect::<Vec<_>>(),
      )
      .await?;
    let page_ids = self.page_index.keys().copied().collect::<Vec<_>>();
    let subkey = self.subkey.clone();

    Ok(
      futures::stream::iter(page_ids)
        .zip(backend_stream)
        .map(move |(page_id, data)| {
          let data = data?;
          let data = subkey.decrypt_with_u64_le_nonce(data.to_vec(), page_id)?;
          Ok::<_, anyhow::Error>((page_id, Bytes::from(data)))
        })
        .boxed(),
    )
  }
}

impl AsRef<BlobReader> for BlobReader {
  fn as_ref(&self) -> &BlobReader {
    self
  }
}
