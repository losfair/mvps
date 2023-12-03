use std::{fs::Metadata, ops::Range, path::PathBuf, pin::Pin, rc::Rc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::interfaces::{ImageInfo, ImageStore, RemoteBlob, RemoteBlobMetadata};

pub const IMAGES_SUBDIR: &str = "images";
pub const BLOBS_SUBDIR: &str = "blobs";

pub struct LocalFsImageStore {
  directory: PathBuf,
}

impl LocalFsImageStore {
  pub fn new(directory: PathBuf) -> anyhow::Result<Self> {
    let directory = directory.canonicalize()?;
    std::fs::create_dir_all(directory.join(IMAGES_SUBDIR))?;
    std::fs::create_dir_all(directory.join(BLOBS_SUBDIR))?;
    Ok(Self { directory })
  }
}

#[async_trait(?Send)]
impl ImageStore for LocalFsImageStore {
  async fn get_image_info(&self, image_id: &str) -> anyhow::Result<ImageInfo> {
    let path = self.directory.join(IMAGES_SUBDIR).join(image_id);
    let raw = match tokio::fs::read(&path).await {
      Ok(x) => x,
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
        return Ok(ImageInfo {
          version: 1,
          change_count: 0,
          layers: vec![],
        });
      }
      Err(e) => return Err(e.into()),
    };
    let info: ImageInfo = serde_json::from_slice(&raw)?;
    Ok(info)
  }

  async fn set_image_info(&self, image_id: &str, info: &ImageInfo) -> anyhow::Result<()> {
    let path = self.directory.join(IMAGES_SUBDIR).join(image_id);
    let raw = serde_json::to_vec(info)?;
    tokio::fs::write(&path, raw).await?;
    Ok(())
  }

  async fn get_blob(self: Rc<Self>, layer_id: &str) -> anyhow::Result<Rc<dyn RemoteBlob>> {
    let path = self.directory.join(BLOBS_SUBDIR).join(layer_id);
    let metadata = tokio::fs::metadata(&path).await?;
    Ok(Rc::new(LocalFsBlob { path, metadata }))
  }

  async fn set_blob(
    &self,
    blob_id: &str,
    mut blob: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>,
  ) -> anyhow::Result<()> {
    let path = self.directory.join(BLOBS_SUBDIR).join(blob_id);
    let tmp_path = path.with_extension("tmp");

    let mut file = tokio::fs::File::create(&tmp_path).await?;
    while let Some(chunk) = blob.next().await {
      let chunk = chunk?;
      file.write_all(&chunk).await?;
    }
    drop(file);

    tokio::fs::rename(&tmp_path, &path).await?;
    Ok(())
  }
}

pub struct LocalFsBlob {
  path: PathBuf,
  metadata: Metadata,
}

#[async_trait(?Send)]
impl RemoteBlob for LocalFsBlob {
  async fn read_metadata(&self) -> anyhow::Result<RemoteBlobMetadata> {
    Ok(RemoteBlobMetadata {
      created_at: self.metadata.created()?.into(),
    })
  }

  async fn read_range(&self, file_offset_range: Range<u64>) -> anyhow::Result<Bytes> {
    let Some(num_readable_bytes) = file_offset_range
      .end
      .min(self.metadata.len())
      .checked_sub(file_offset_range.start)
    else {
      anyhow::bail!("read_range: invalid range")
    };
    let mut file = tokio::fs::File::open(&self.path).await?;
    let mut buf = vec![0u8; (file_offset_range.end - file_offset_range.start) as usize];
    file
      .seek(std::io::SeekFrom::Start(file_offset_range.start))
      .await?;
    file
      .read_exact(&mut buf[..num_readable_bytes as usize])
      .await?;
    // the rest of the buffer are left zeroed
    Ok(buf.into())
  }

  async fn stream_chunks(
    self: Rc<Self>,
    file_offset_start: u64,
    chunk_sizes: Vec<u64>,
  ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>> {
    let mut file = tokio::fs::File::open(&self.path).await?;
    file
      .seek(std::io::SeekFrom::Start(file_offset_start))
      .await?;

    let stream = async_stream::try_stream! {
      for chunk_size in chunk_sizes {
        let mut buf = vec![0; chunk_size as usize];
        file.read_exact(&mut buf).await?;
        yield buf.into();
      }
    };

    Ok(stream.boxed())
  }
}
