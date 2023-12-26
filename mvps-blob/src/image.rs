use std::{rc::Rc, time::Duration};

use bytestring::ByteString;
use futures::{StreamExt, TryStreamExt};

use crate::{
  blob_crypto::CryptoRootKey,
  blob_reader::{BlobReader, DecompressedPage, PagePresence},
  interfaces::{ImageInfo, ImageStore},
};

const LAYER_LOAD_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct ImageManager {
  pub store: Rc<dyn ImageStore>,
  pub image_id: ByteString,
  pub layers: Vec<Rc<Layer>>,

  pub writer_id: Option<ByteString>,

  pub change_count: u64,
}

pub struct Layer {
  pub blob_id: ByteString,
  pub blob: BlobReader,
}

impl ImageManager {
  pub async fn open(
    store: Rc<dyn ImageStore>,
    image_id: ByteString,
    decryption_keys: &[CryptoRootKey],
  ) -> anyhow::Result<Self> {
    let image_info = store.get_image_info(&image_id).await?;
    if image_info.version != 1 {
      anyhow::bail!("unsupported image info version");
    }

    let layers = futures::stream::iter(image_info.layers.iter().map(|blob_id| {
      let store = store.clone();
      async move {
        Ok::<_, anyhow::Error>(Rc::new(Layer {
          blob_id: blob_id.clone(),
          blob: BlobReader::open(store.get_blob(blob_id).await?, decryption_keys).await?,
        }))
      }
    }))
    .buffered(LAYER_LOAD_CONCURRENCY)
    .try_collect::<Vec<Rc<Layer>>>()
    .await?;

    tracing::info!(
      change_count = image_info.change_count,
      num_layers = layers.len(),
      writer_id = image_info.writer_id.as_deref().unwrap_or_default(),
      "opened image"
    );

    Ok(Self {
      store,
      image_id,
      layers,
      writer_id: image_info.writer_id,
      change_count: image_info.change_count,
    })
  }

  pub async fn read_page(&self, id: u64) -> anyhow::Result<Option<DecompressedPage>> {
    for layer in self.layers.iter().rev() {
      let presence = layer.blob.read_page(id).await?;
      match presence {
        PagePresence::Present(x) => return Ok(Some(x)),
        PagePresence::Tombstone => return Ok(None),
        PagePresence::NotPresent => {}
      }
    }

    Ok(None)
  }

  pub fn push_layer(&mut self, blob_id: ByteString, blob: BlobReader) -> anyhow::Result<()> {
    self.layers.push(Rc::new(Layer { blob_id, blob }));
    Ok(())
  }

  pub async fn write_back(&self) -> anyhow::Result<()> {
    let start_time = std::time::Instant::now();
    let ret = tokio::time::timeout(
      Duration::from_secs(60),
      self.store.set_image_info(
        &self.image_id,
        &ImageInfo {
          version: 1,
          change_count: self.change_count,
          writer_id: self.writer_id.clone(),
          layers: self.layers.iter().map(|x| x.blob_id.clone()).collect(),
        },
      ),
    )
    .await;
    match ret {
      Ok(ret) => ret?,
      Err(tokio::time::error::Elapsed { .. }) => {
        // We want the image to be written back within a reasonable time. Otherwise if the interval between blob write
        // and image write is too long, GC may think the blob is no longer in use and delete it, corrupting the image.
        tracing::error!(image_id = %self.image_id, change_count = self.change_count, "timed out writing back image");
        panic!("timed out writing back image, no longer safe to retry");
      }
    }
    tracing::info!(image_id = %self.image_id, num_layers = self.layers.len(), change_count = self.change_count, duration = ?start_time.elapsed(), "written back image");
    Ok(())
  }
}
