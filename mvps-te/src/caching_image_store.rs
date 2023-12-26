use std::{
  ops::Range,
  pin::Pin,
  rc::Rc,
  sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use bytestring::ByteString;
use futures::{stream::FuturesOrdered, Stream, TryStreamExt};
use memmap2::{MmapOptions, MmapRaw};
use mvps_blob::interfaces::{ImageInfo, ImageStore, RemoteBlob, RemoteBlobMetadata};
use rand::Rng;
use slab::Slab;

use crate::io_planner::IoPlannerIterator;

pub struct CachingImageStore {
  inner: Rc<dyn ImageStore>,
  block_size: u64,
  cache: moka::future::Cache<(ByteString, u64), Arc<MappedHandle>>, // (blob_id, block_id) -> mapped_id

  mapped_cache: Arc<MappedCache>,
}

struct MappedHandle {
  mapped_cache: Arc<MappedCache>,
  mapped_id: usize,
}

impl Drop for MappedHandle {
  fn drop(&mut self) {
    self
      .mapped_cache
      .alloc
      .lock()
      .unwrap()
      .remove(self.mapped_id);
  }
}

impl MappedHandle {
  async fn read(&self, range: Range<u64>) -> Vec<u8> {
    let mapped_cache = self.mapped_cache.clone();
    let read_offset = self.mapped_id * self.mapped_cache.block_size as usize + range.start as usize;

    let data = tokio::task::spawn_blocking(move || {
      let range_len = range.clone().count();
      let mut output = Vec::with_capacity(range_len);
      unsafe {
        assert!(read_offset.saturating_add(range_len) <= mapped_cache.mapping.len());
        std::ptr::copy_nonoverlapping(
          mapped_cache.mapping.as_ptr().offset(read_offset as isize),
          output.as_mut_ptr(),
          range_len,
        );
        output.set_len(range_len);
      }
      output
    })
    .await
    .unwrap();
    data
  }
}

struct MappedCache {
  mapping: MmapRaw,
  alloc: Mutex<Slab<()>>,
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
    let mapping = MmapOptions::new().map_raw(&mapping_file)?;
    drop(mapping_file);

    let mapped_cache = Arc::new(MappedCache {
      mapping,
      alloc: Mutex::new(Slab::with_capacity(mapped_cache_size_blocks as usize)),
      block_size: config.block_size as usize,
    });
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

  async fn read_range(&self, file_offset_range: Range<u64>) -> anyhow::Result<Vec<u8>> {
    if file_offset_range.is_empty() {
      return Ok(vec![]);
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
              let mut alloc = self.store.mapped_cache.alloc.lock().unwrap();
              if alloc.len() == alloc.capacity() {
                drop(alloc);
                if attempts >= 20 {
                  tracing::warn!(attempts, "mapped cache full, backing off");
                }
                attempts += 1;
                self.store.cache.run_pending_tasks().await;
                tokio::time::sleep(std::time::Duration::from_millis(
                  100 + rand::thread_rng().gen_range(0..100),
                ))
                .await;
                continue;
              }

              let mapped_id = alloc.insert(());

              // Cache entry should be deallocated if subsequent async operations are cancelled
              let handle = Arc::new(MappedHandle {
                mapped_cache: self.store.mapped_cache.clone(),
                mapped_id,
              });
              drop(alloc);

              let write_offset = mapped_id * self.store.block_size as usize;
              let handle2 = handle.clone();

              tokio::task::spawn_blocking(move || {
                unsafe {
                  assert!(
                    write_offset.saturating_add(data.len()) <= handle2.mapped_cache.mapping.len()
                  );
                  std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    handle2
                      .mapped_cache
                      .mapping
                      .as_mut_ptr()
                      .offset(write_offset as isize),
                    data.len(),
                  );
                }

                // Hold the handle until memcpy completes
                drop(handle2);
              })
              .await
              .unwrap();

              break Ok::<_, String>(handle);
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
      return Ok(block.read(range).await);
    }

    let file_offset_range_2 = file_offset_range.clone();
    let mut result = Vec::with_capacity(file_offset_range_2.count() as usize);
    for (block, range) in blocks {
      let data = block.read(range).await;
      result.extend_from_slice(&data[..]);
    }

    assert_eq!(result.len(), file_offset_range.count() as usize);
    Ok(result)
  }

  async fn stream_raw_chunks(
    self: Rc<Self>,
    file_offset_start: u64,
    chunk_sizes: Vec<u64>,
  ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>> {
    self
      .inner
      .clone()
      .stream_raw_chunks(file_offset_start, chunk_sizes)
      .await
  }
}
