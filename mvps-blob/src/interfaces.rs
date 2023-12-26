use std::{ops::Range, pin::Pin, rc::Rc};

use async_trait::async_trait;
use bytes::Bytes;
use bytestring::ByteString;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ImageInfo {
  pub version: i32,
  pub change_count: u64,

  #[serde(default)]
  pub writer_id: Option<ByteString>,

  #[serde(default)]
  pub layers: Vec<ByteString>,
}

#[async_trait(?Send)]
pub trait ImageStore {
  async fn get_image_info(&self, image_id: &str) -> anyhow::Result<ImageInfo>;
  async fn set_image_info(&self, image_id: &str, info: &ImageInfo) -> anyhow::Result<()>;
  async fn get_blob(self: Rc<Self>, blob_id: &str) -> anyhow::Result<Rc<dyn RemoteBlob>>;
  async fn set_blob(
    &self,
    blob_id: &str,
    blob: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>,
  ) -> anyhow::Result<()>;
}

#[async_trait(?Send)]
pub trait RemoteBlob {
  async fn read_metadata(&self) -> anyhow::Result<RemoteBlobMetadata>;
  async fn read_range(&self, file_offset_range: Range<u64>) -> anyhow::Result<Vec<u8>>;
  async fn stream_raw_chunks(
    self: Rc<Self>,
    file_offset_start: u64,
    chunk_sizes: Vec<u64>,
  ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>>;
}

#[derive(Clone, Debug)]
pub struct RemoteBlobMetadata {
  pub created_at: DateTime<Utc>,
}
