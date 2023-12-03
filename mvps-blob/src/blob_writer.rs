use std::ops::Deref;

use bytes::Bytes;
use mvps_proto::blob::BlobHeader;
use prost::Message;

use crate::consts::BLOB_MAGIC;

pub struct BlobHeaderWriter {
  output: BlobHeader,
  last_page: LastPage,
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

impl BlobHeaderWriter {
  pub fn new(options: BlobHeaderWriterOptions) -> Self {
    Self {
      output: BlobHeader {
        version: 1,
        page_ids_delta_encoded: vec![],
        page_compressed_sizes_delta_encoded: vec![],
        metadata: options.metadata,
      },
      last_page: LastPage {
        id: None,
        compressed_size: 0,
      },
    }
  }

  pub fn into_inner(self) -> BlobHeader {
    self.output
  }

  pub fn encode(&self) -> Bytes {
    let header_encoded = zstd::encode_all(&self.output.encode_to_vec()[..], 0).unwrap();
    Bytes::from(
      [
        BLOB_MAGIC,
        &(header_encoded.len() as u32).to_le_bytes(),
        &header_encoded,
      ]
      .concat(),
    )
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
