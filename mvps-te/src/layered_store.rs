use std::{cell::RefCell, io::SeekFrom, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use futures::{future::ready, StreamExt, TryStreamExt};
use heed::Env;
use mvps_blob::{
  blob_reader::{BlobReader, PagePresence},
  blob_writer::{BlobHeaderWriter, BlobHeaderWriterOptions, PageInfoInHeader},
  image::ImageManager,
  interfaces::ImageStore,
  util::generate_blob_id,
};
use mvps_proto::blob::{BlobPage, BlobPageCompressionMethod};
use prost::Message;
use tokio::{
  io::{AsyncSeekExt, AsyncWriteExt},
  sync::{watch, RwLock},
  task::{spawn_blocking, JoinHandle},
};

use crate::{
  bufferstore::{BufferStore, OwnedBufferStoreTxn},
  compaction::trim_and_compact_once,
  config::{JitteredInterval, LayeredStoreConfig},
};

pub struct LayeredStore {
  bs: Option<Rc<BufferStore>>,
  tasks: Vec<JoinHandle<()>>,
  inner: Rc<Inner>,
}

struct Inner {
  writeback_req_tx: watch::Sender<u64>,
  writeback_ack_rx: watch::Receiver<u64>,
  base: RefCell<ImageManager>,
  config: LayeredStoreConfig,
  shutdown: Rc<RwLock<()>>,
  checkpoint_req_tx: tokio::sync::watch::Sender<u64>,
  checkpoint_ack_rx: tokio::sync::watch::Receiver<u64>,
}

impl LayeredStore {
  pub async fn new(
    image_store: Rc<dyn ImageStore>,
    image_id: ByteString,
    env: Option<Env>,
    config: LayeredStoreConfig,
  ) -> anyhow::Result<Self> {
    let (writeback_req_tx, writeback_req_rx) = watch::channel(0u64);
    let (writeback_ack_tx, writeback_ack_rx) = watch::channel(0u64);
    let (checkpoint_req_tx, checkpoint_req_rx) = tokio::sync::watch::channel(0u64);
    let (checkpoint_ack_tx, checkpoint_ack_rx) = tokio::sync::watch::channel(0u64);

    let shutdown = Rc::new(RwLock::new(()));

    let bs = if let Some(env) = env {
      Some(Rc::new(BufferStore::new(env, image_id.clone())?))
    } else {
      None
    };
    let base = RefCell::new(ImageManager::open(image_store, image_id).await?);
    if let Some(bs) = &bs {
      let image_change_count = base.borrow().change_count;
      let buffer_store_change_count = bs.advance_change_count(image_change_count)?;
      tracing::info!(
        image_change_count,
        buffer_store_change_count,
        "advanced change count"
      );
    }
    let inner = Rc::new(Inner {
      base,
      config,
      writeback_req_tx,
      writeback_ack_rx,
      shutdown: shutdown.clone(),
      checkpoint_req_tx,
      checkpoint_ack_rx,
    });
    let mut me = Self {
      bs,
      tasks: vec![],
      inner: inner.clone(),
    };

    if me.bs.is_some() && !me.inner.config.disable_image_store_write {
      me.tasks = vec![
        tokio::task::spawn_local(checkpoint_loop(
          Rc::downgrade(&inner),
          me.bs.as_ref().unwrap().clone(),
          checkpoint_req_rx,
          checkpoint_ack_tx,
          shutdown.clone(),
        )),
        tokio::task::spawn_local(compaction_loop(Rc::downgrade(&inner), shutdown.clone())),
        tokio::task::spawn_local(writeback_loop(
          Rc::downgrade(&inner),
          writeback_req_rx,
          writeback_ack_tx,
          shutdown,
        )),
      ];
    }
    Ok(me)
  }

  pub fn begin_txn(&self) -> anyhow::Result<LayeredStoreTxn> {
    Ok(LayeredStoreTxn {
      bs: self
        .bs
        .as_ref()
        .map(|x| x.clone().begin_txn_owned())
        .transpose()?,
      base: self.inner.base.borrow().clone(),
    })
  }

  pub async fn request_checkpoint(&self, wait: bool) -> anyhow::Result<()> {
    if self.bs.is_some() {
      let mut seq = 0u64;
      self.inner.checkpoint_req_tx.send_modify(|x| {
        *x += 1;
        seq = *x;
      });

      if !wait {
        return Ok(());
      }

      let mut checkpoint_ack_rx = self.inner.checkpoint_ack_rx.clone();
      loop {
        if *checkpoint_ack_rx.borrow() >= seq {
          break;
        }
        checkpoint_ack_rx.changed().await?;
      }
    }
    Ok(())
  }

  pub async fn shutdown(mut self) -> anyhow::Result<()> {
    self.bs.take();

    let _guard = self.inner.shutdown.write().await;
    for task in &self.tasks {
      task.abort();
    }
    for task in self.tasks {
      let _ = task.await;
    }

    Ok(())
  }

  pub fn buffer_store_size(&self) -> anyhow::Result<u64> {
    Ok(self.bs.as_ref().map(|x| x.len()).transpose()?.unwrap_or(0))
  }

  #[cfg(test)]
  pub fn test_get_base(&self) -> ImageManager {
    self.inner.base.borrow().clone()
  }
}

pub struct LayeredStoreTxn {
  bs: Option<OwnedBufferStoreTxn>,
  base: ImageManager,
}

impl LayeredStoreTxn {
  pub async fn read_page(&self, page_id: u64) -> anyhow::Result<Option<Bytes>> {
    if let Some(bs) = &self.bs {
      match bs.get().read_page(page_id).await? {
        PagePresence::Present(x) => return Ok(Some(x.data)),
        PagePresence::Tombstone => return Ok(None),
        PagePresence::NotPresent => {}
      }
    }

    Ok(self.base.read_page(page_id).await?.map(|x| x.data))
  }

  pub async fn lock_for_write(&mut self) -> anyhow::Result<bool> {
    let Some(bs) = &mut self.bs else {
      anyhow::bail!("cannot lock read-only transaction");
    };

    bs.get_mut().lock_for_write().await
  }

  pub async fn write_page(&mut self, page_id: u64, data: Option<Bytes>) -> anyhow::Result<()> {
    let Some(bs) = &mut self.bs else {
      anyhow::bail!("cannot write to read-only transaction");
    };

    bs.get_mut().write_page(page_id, data).await
  }

  pub async fn commit(mut self) -> anyhow::Result<()> {
    let Some(bs) = self.bs.take() else {
      return Ok(());
    };
    bs.commit().await
  }

  pub async fn rollback(self) -> anyhow::Result<()> {
    Ok(())
  }
}

impl Inner {
  fn begin_writeback(&self) -> u64 {
    let mut writeback_seq = 0u64;
    self.writeback_req_tx.send_modify(|x| {
      *x += 1;
      writeback_seq = *x;
    });
    writeback_seq
  }

  async fn writeback(&self) -> anyhow::Result<()> {
    let writeback_seq = self.begin_writeback();
    let mut writeback_ack_rx = self.writeback_ack_rx.clone();
    while *writeback_ack_rx.borrow() < writeback_seq {
      writeback_ack_rx.changed().await?;
    }

    Ok(())
  }
}

async fn checkpoint_loop(
  me: std::rc::Weak<Inner>,
  bs: Rc<BufferStore>,
  mut checkpoint_req_rx: tokio::sync::watch::Receiver<u64>,
  checkpoint_ack_tx: tokio::sync::watch::Sender<u64>,
  shutdown: Rc<RwLock<()>>,
) {
  loop {
    if checkpoint_req_rx.changed().await.is_err() {
      return;
    }

    let _guard = shutdown.read().await;

    let Some(me) = me.upgrade() else {
      return;
    };

    let seq = *checkpoint_req_rx.borrow();
    if let Err(e) = checkpoint_once(me, &bs).await {
      tracing::error!(error = ?e, "failed to checkpoint");
      continue;
    }

    let _ = checkpoint_ack_tx.send(seq);
  }
}

async fn checkpoint_once(me: Rc<Inner>, bs: &BufferStore) -> anyhow::Result<()> {
  let snapshot = bs.snapshot_for_checkpoint()?;

  let mut page_change_counts: Vec<(u64, u64)> = vec![];
  let mut header = BlobHeaderWriter::new(BlobHeaderWriterOptions {
    metadata: "".into(),
  });

  let mut tempfile = tokio::fs::File::from_std(spawn_blocking(|| tempfile::tempfile()).await??);
  for x in snapshot.iter()? {
    tokio::task::yield_now().await;
    let (page_id, change_count, data) = x?;
    page_change_counts.push((page_id, change_count));
    if let Some(data) = data {
      let data = BlobPage {
        compression: BlobPageCompressionMethod::BpcmZstd.into(),
        data: Bytes::from(spawn_blocking(move || zstd::encode_all(&data[..], 0).unwrap()).await?),
      }
      .encode_to_vec();
      tempfile.write_all(&data[..]).await?;
      header.add_page(PageInfoInHeader {
        id: page_id,
        compressed_size: data.len() as u64,
      })?;
    } else {
      header.add_page(PageInfoInHeader {
        id: page_id,
        compressed_size: 0,
      })?;
    }
  }
  if page_change_counts.len() > 10000 {
    tracing::warn!(num_pages = page_change_counts.len(), "large checkpoint");
  }

  if page_change_counts.is_empty() {
    return Ok(());
  }

  let store = me.base.borrow().store.clone();
  let new_blob_id = ByteString::from(generate_blob_id());

  tempfile.seek(SeekFrom::Start(0)).await?;

  store
    .set_blob(
      &new_blob_id,
      futures::stream::once(ready(Ok(header.encode())))
        .chain(tokio_util::io::ReaderStream::new(tempfile).map_err(anyhow::Error::from))
        .boxed(),
    )
    .await?;

  let reader = BlobReader::open(store.get_blob(&new_blob_id).await?).await?;

  {
    let mut base_ref = me.base.borrow_mut();
    base_ref.push_layer(new_blob_id.clone(), reader)?;
    base_ref.change_count = snapshot.change_count();
  }

  me.writeback().await?;

  drop(snapshot);
  bs.unbuffer_pages(page_change_counts)?;

  Ok(())
}

async fn compaction_loop(me: std::rc::Weak<Inner>, shutdown: Rc<RwLock<()>>) {
  let mut interval: Option<JitteredInterval> = None;
  loop {
    if let Some(interval) = interval {
      interval.sleep().await;
    }

    let _guard = shutdown.read().await;

    let Some(me) = me.upgrade() else {
      return;
    };
    if interval.is_none() {
      interval = Some(me.config.compaction_interval);
    }

    trim_and_compact_once(
      |f| {
        let mut im = me.base.borrow_mut();
        f(&mut *im);
      },
      || {
        me.begin_writeback();
      },
      &me.config.compaction_thresholds,
    )
    .await;
  }
}

async fn writeback_loop(
  me: std::rc::Weak<Inner>,
  mut writeback_req_rx: watch::Receiver<u64>,
  writeback_ack_tx: watch::Sender<u64>,
  shutdown: Rc<RwLock<()>>,
) {
  let mut current_seq = 0u64;

  loop {
    let received_seq = *writeback_req_rx.borrow();
    if received_seq == current_seq {
      if writeback_req_rx.changed().await.is_err() {
        return;
      }
      continue;
    }
    current_seq = received_seq;

    let _guard = shutdown.read().await;

    let Some(me) = me.upgrade() else {
      return;
    };

    let im = me.base.borrow().clone();
    if let Err(e) = im.write_back().await {
      tracing::error!(error = ?e, "failed to write back image");
      return;
    }

    let _ = writeback_ack_tx.send(current_seq);
  }
}
