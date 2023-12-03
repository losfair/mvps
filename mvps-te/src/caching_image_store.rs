use std::{
  ops::Range,
  pin::Pin,
  rc::Rc,
  sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures::{stream::FuturesOrdered, Stream, TryStreamExt};
use memmap2::{MmapMut, MmapOptions};
use mvps_blob::interfaces::{ImageInfo, ImageStore, RemoteBlob, RemoteBlobMetadata};
use slab::Slab;

use crate::io_planner::IoPlannerIterator;

pub struct CachingImageStore {
  inner: Rc<dyn ImageStore>,
  block_size: u64,
  cache: moka::future::Cache<(ByteString, u64), Arc<MappedHandle>>, // (blob_id, block_id) -> mapped_id

  mapped_cache: Arc<Mutex<MappedCache>>,
}

struct MappedHandle {
  mapped_cache: Arc<Mutex<MappedCache>>,
  mapped_id: usize,
}

impl Drop for MappedHandle {
  fn drop(&mut self) {
    let mut mapped = self.mapped_cache.lock().unwrap();
    mapped.alloc.remove(self.mapped_id);
  }
}

impl MappedHandle {
  fn read(&self, range: Range<u64>) -> Bytes {
    let mut output = BytesMut::with_capacity(range.clone().count());
    self.read_to(range, &mut output);
    output.freeze()
  }

  fn read_to(&self, range: Range<u64>, output: &mut BytesMut) {
    let mapped = self.mapped_cache.lock().unwrap();
    let slice = &mapped.mapping
      [self.mapped_id * mapped.block_size as usize + range.start as usize..][..range.count()];
    output.extend_from_slice(slice);
  }
}

struct MappedCache {
  mapping: MmapMut,
  alloc: Slab<()>,
  block_size: usize,
}

#[derive(Clone, Debug)]
pub struct CachingImageStoreConfig {
  pub block_size: u64,
  pub cache_size: u64,
}

impl CachingImageStore {
  pub fn new(inner: Rc<dyn ImageStore>, config: CachingImageStoreConfig) -> anyhow::Result<Self> {
    if config.cache_size == 0
      || config.block_size == 0
      || config.cache_size % config.block_size != 0
    {
      anyhow::bail!("invalid cache_size or block_size");
    }

    // leave some headroom
    let mapped_cache_size_blocks = config.cache_size / config.block_size + 64;

    let mapping_file = tempfile::tempfile()?;
    mapping_file.set_len(mapped_cache_size_blocks * config.block_size)?;
    let mapping = unsafe { MmapOptions::new().map_mut(&mapping_file)? };
    drop(mapping_file);

    let mapped_cache = Arc::new(Mutex::new(MappedCache {
      mapping,
      alloc: Slab::with_capacity(mapped_cache_size_blocks as usize),
      block_size: config.block_size as usize,
    }));
    let block_size = config.block_size;

    Ok(Self {
      inner,
      block_size: config.block_size,
      cache: moka::future::Cache::builder()
        .max_capacity(config.cache_size)
        .weigher(move |_, _| 128 + block_size as u32)
        .build(),
      mapped_cache,
    })
  }
}

#[async_trait(?Send)]
impl ImageStore for CachingImageStore {
  async fn get_image_info(&self, image_id: &str) -> anyhow::Result<ImageInfo> {
    self.inner.get_image_info(image_id).await
  }

  async fn set_image_info(&self, image_id: &str, info: &ImageInfo) -> anyhow::Result<()> {
    self.inner.set_image_info(image_id, info).await
  }

  async fn get_blob(self: Rc<Self>, blob_id: &str) -> anyhow::Result<Rc<dyn RemoteBlob>> {
    let inner = self.inner.clone();
    let blob_id = ByteString::from(blob_id);
    Ok(Rc::new(CachingBlob {
      store: self,
      inner: inner.get_blob(&blob_id).await?,
      blob_id,
    }))
  }

  async fn set_blob(
    &self,
    blob_id: &str,
    blob: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>,
  ) -> anyhow::Result<()> {
    self.inner.set_blob(blob_id, blob).await
  }
}

pub struct CachingBlob {
  store: Rc<CachingImageStore>,
  inner: Rc<dyn RemoteBlob>,
  blob_id: ByteString,
}

#[async_trait(?Send)]
impl RemoteBlob for CachingBlob {
  async fn read_metadata(&self) -> anyhow::Result<RemoteBlobMetadata> {
    self.inner.read_metadata().await
  }

  async fn read_range(&self, file_offset_range: Range<u64>) -> anyhow::Result<Bytes> {
    if file_offset_range.is_empty() {
      return Ok(Bytes::new());
    }

    let it = IoPlannerIterator::new(self.store.block_size, file_offset_range.clone());
    let blocks = it
      .map(|(block_id, range_in_block)| async move {
        let block = self
          .store
          .cache
          .try_get_with((self.blob_id.clone(), block_id), async move {
            let data = self
              .inner
              .read_range(block_id * self.store.block_size..(block_id + 1) * self.store.block_size)
              .await
              .map_err(|e| format!("read_range error: {:?}", e))?;
            assert_eq!(data.len(), self.store.block_size as usize);
            let mut attempts = 0u64;
            loop {
              let mut mapped_cache = self.store.mapped_cache.lock().unwrap();
              if mapped_cache.alloc.len() == mapped_cache.alloc.capacity() {
                drop(mapped_cache);
                if attempts >= 3 {
                  tracing::warn!(attempts, "mapped cache full, backing off");
                }
                attempts += 1;
                self.store.cache.run_pending_tasks().await;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
              }

              let mapped_id = mapped_cache.alloc.insert(());
              mapped_cache.mapping[mapped_id * self.store.block_size as usize..][..data.len()]
                .copy_from_slice(&data);
              break Ok::<_, String>(Arc::new(MappedHandle {
                mapped_cache: self.store.mapped_cache.clone(),
                mapped_id,
              }));
            }
          })
          .await
          .map_err(|e| anyhow::anyhow!("failed to load block from underlying store: {:?}", e))?;
        Ok::<_, anyhow::Error>((block, range_in_block))
      })
      .collect::<FuturesOrdered<_>>()
      .try_collect::<Vec<_>>()
      .await?;

    // Fast path for one block
    if blocks.len() == 1 {
      let (block, range) = blocks.into_iter().next().unwrap();
      return Ok(block.read(range));
    }

    let mut result = BytesMut::with_capacity(file_offset_range.clone().count() as usize);
    for (block, range) in blocks {
      block.read_to(range, &mut result);
    }
    assert_eq!(result.len(), file_offset_range.count() as usize);
    Ok(result.freeze())
  }

  async fn stream_chunks(
    self: Rc<Self>,
    file_offset_start: u64,
    chunk_sizes: Vec<u64>,
  ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>> {
    self
      .inner
      .clone()
      .stream_chunks(file_offset_start, chunk_sizes)
      .await
  }
}
