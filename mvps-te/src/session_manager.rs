use std::{
  cell::RefCell,
  collections::HashMap,
  num::NonZeroU32,
  ops::Range,
  path::PathBuf,
  pin::Pin,
  rc::Rc,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use bytes::Bytes;
use bytestring::ByteString;
use futures::{future::Either, Future, FutureExt, StreamExt, TryStreamExt};
use governor::{Quota, RateLimiter};
use heed::Env;
use mvps_blob::{
  backend::local_fs::LocalFsImageStore, blob_crypto::CryptoRootKey, interfaces::ImageStore,
  util::is_valid_image_id,
};
use rand::Rng;
use tokio::{
  io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
  sync::{
    mpsc::{Receiver, Sender, UnboundedReceiver},
    oneshot, watch, OwnedMutexGuard, RwLock, Semaphore,
  },
  task::spawn_local,
  time::MissedTickBehavior,
};

use crate::{
  caching_image_store::{CachingImageStore, CachingImageStoreConfig},
  config::{ImageStoreProvider, JitteredInterval, LayeredStoreConfig},
  io_planner::IoPlannerIterator,
  layered_store::{LayeredStore, LayeredStoreTxn},
  nbd::EstablishedConn,
};

pub struct SessionManager {
  sessions: Mutex<Option<HashMap<ByteString, Session>>>, // image_id -> session
  config: SessionManagerConfig,
}

#[derive(Clone)]
pub struct SessionManagerConfig {
  pub image_store_provider: ImageStoreProvider,
  pub bs_env: Option<Env>,
  pub layered_store_config: LayeredStoreConfig,
  pub checkpoint_interval: JitteredInterval,
  pub image_cache_size: u64,
  pub image_cache_block_size: u64,
  pub write_throttle_threshold_pages: u64,
  pub root_key: Arc<Option<CryptoRootKey>>,
  pub decryption_keys: Arc<Vec<CryptoRootKey>>,
}

impl SessionManager {
  pub fn new(config: SessionManagerConfig) -> Self {
    Self {
      sessions: Mutex::new(Some(HashMap::new())),
      config,
    }
  }

  pub fn cleanup(&self) {
    let mut sessions = self.sessions.lock().unwrap();
    if let Some(sessions) = &mut *sessions {
      sessions.retain(|_, x| !x.handle.is_finished());
    }
  }

  pub fn shutdown_blocking(&self) {
    let Some(sessions) = self.sessions.lock().unwrap().take() else {
      return;
    };

    // Here `shutdown_tx` of all sessions are dropped
    let handles = sessions.into_values().map(|x| x.handle).collect::<Vec<_>>();

    for h in handles {
      let _ = h.join();
    }
  }

  pub fn get_session(&self, image_id: ByteString) -> anyhow::Result<SessionHandle> {
    if !is_valid_image_id(&image_id) {
      anyhow::bail!("invalid image id");
    }

    let mut sessions = self.sessions.lock().unwrap();
    let Some(sessions) = &mut *sessions else {
      anyhow::bail!("session manager is shutting down");
    };
    let session = sessions.entry(image_id.clone()).or_insert_with(|| {
      let (conn_tx, conn_rx) = tokio::sync::mpsc::channel(1);
      let (shutdown_tx, shutdown_rx) = oneshot::channel();
      let config = Arc::new(SessionConfig {
        image_id: image_id.clone(),
        image_store_provider: self.config.image_store_provider.clone(),
        bs_env: self.config.bs_env.clone(),
        layered_store_config: self.config.layered_store_config.clone(),
        checkpoint_interval: self.config.checkpoint_interval,
        image_cache_size: self.config.image_cache_size,
        image_cache_block_size: self.config.image_cache_block_size,
        write_throttle_threshold_pages: self.config.write_throttle_threshold_pages,
        root_key: self.config.root_key.clone(),
        decryption_keys: self.config.decryption_keys.clone(),
      });
      let inner = Arc::new(SessionInner {});
      let handle = std::thread::Builder::new()
        .name(format!("i-{}", image_id))
        .spawn(move || {
          let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name(format!("w-{}", image_id))
            .max_blocking_threads(4)
            .build()
            .unwrap();
          let local_set = tokio::task::LocalSet::new();
          local_set.block_on(&rt, session_loop(inner, config, conn_rx, shutdown_rx));
        })
        .unwrap();
      Session {
        conn_tx,
        _shutdown_tx: shutdown_tx,
        handle,
      }
    });
    Ok(SessionHandle {
      conn_tx: session.conn_tx.clone(),
    })
  }
}

struct Session {
  conn_tx: Sender<EstablishedConn>,
  _shutdown_tx: oneshot::Sender<()>,
  handle: std::thread::JoinHandle<()>,
}

pub struct SessionHandle {
  conn_tx: Sender<EstablishedConn>,
}

impl SessionHandle {
  pub async fn handover(&self, conn: EstablishedConn) -> anyhow::Result<()> {
    self
      .conn_tx
      .send(conn)
      .await
      .map_err(|_| anyhow::anyhow!("session is closed"))?;
    Ok(())
  }
}

struct SessionConfig {
  image_id: ByteString,
  image_store_provider: ImageStoreProvider,
  bs_env: Option<Env>,
  layered_store_config: LayeredStoreConfig,
  checkpoint_interval: JitteredInterval,
  image_cache_size: u64,
  image_cache_block_size: u64,
  write_throttle_threshold_pages: u64,

  root_key: Arc<Option<CryptoRootKey>>,
  decryption_keys: Arc<Vec<CryptoRootKey>>,
}
struct SessionInner {}

async fn session_loop(
  _inner: Arc<SessionInner>,
  config: Arc<SessionConfig>,
  mut conn_rx: Receiver<EstablishedConn>,
  mut shutdown_rx: oneshot::Receiver<()>,
) {
  let start_time = Instant::now();
  let image_store: Rc<dyn ImageStore> = match &config.image_store_provider {
    ImageStoreProvider::LocalFs { path } => {
      let path: &str = path.as_ref();
      let base_store = Rc::new(match LocalFsImageStore::new(PathBuf::from(path)) {
        Ok(x) => x,
        Err(e) => {
          tracing::error!(image_id = %config.image_id, error = ?e, "failed to open image store");
          return;
        }
      });
      let caching_store = match CachingImageStore::new(
        base_store,
        CachingImageStoreConfig {
          block_size: config.image_cache_block_size,
          cache_size: config.image_cache_size,
        },
      ) {
        Ok(x) => x,
        Err(e) => {
          tracing::error!(image_id = %config.image_id, error = ?e, "failed to initialize cache");
          return;
        }
      };
      Rc::new(caching_store)
    }
    ImageStoreProvider::S3 { bucket, prefix } => {
      let base_store =
        Rc::new(mvps_blob::backend::s3::S3ImageStore::new(bucket.clone(), prefix.clone()).await);
      let caching_store = match CachingImageStore::new(
        base_store,
        CachingImageStoreConfig {
          block_size: config.image_cache_block_size,
          cache_size: config.image_cache_size,
        },
      ) {
        Ok(x) => x,
        Err(e) => {
          tracing::error!(image_id = %config.image_id, error = ?e, "failed to initialize cache");
          return;
        }
      };
      Rc::new(caching_store)
    }
  };
  let layered_store = Rc::new(
    match LayeredStore::new(
      image_store,
      config.image_id.clone(),
      config.bs_env.clone(),
      config.layered_store_config.clone(),
      config.root_key.clone(),
      config.decryption_keys.clone(),
    )
    .await
    {
      Ok(x) => x,
      Err(e) => {
        tracing::error!(image_id = %config.image_id, error = ?e, "failed to open layered store");
        return;
      }
    },
  );

  let periodic_checkpoint_task = {
    let layered_store = layered_store.clone();
    let image_id = config.image_id.clone();
    let checkpoint_interval = config.checkpoint_interval;
    let disable_image_store_write = config.layered_store_config.disable_image_store_write;

    spawn_local(async move {
      if disable_image_store_write {
        futures::future::pending::<()>().await;
        unreachable!();
      }

      loop {
        let next_deadline = Instant::now() + checkpoint_interval.generate();
        if let Err(e) = layered_store.request_checkpoint(true).await {
          tracing::error!(%image_id, error = ?e, "failed to request checkpoint");
        }
        tokio::time::sleep_until(next_deadline.into()).await;
      }
    })
  };

  tracing::warn!(image_id = %config.image_id, init_duration = ?start_time.elapsed(), "session is ready");
  let mut next_conn_id = 1u64;
  let conn_tasks: Rc<RefCell<HashMap<u64, (watch::Sender<()>, Arc<tokio::sync::Mutex<()>>)>>> =
    Rc::new(RefCell::new(HashMap::new()));
  let client_conns: Rc<RefCell<HashMap<ByteString, u64>>> = Rc::new(RefCell::new(HashMap::new()));
  let concurrency_limiter = Arc::new(Semaphore::new(16777216));

  loop {
    let conn = tokio::select! {
      conn = conn_rx.recv() => match conn {
        Some(x) => x,
        None => break,
      },
      _ = &mut shutdown_rx => break,
    };

    let conn_id = next_conn_id;
    next_conn_id += 1;

    let client_id = conn.client_id.clone();
    let layered_store = layered_store.clone();
    let config = config.clone();
    let conn_tasks_2 = conn_tasks.clone();
    let client_conns_2 = client_conns.clone();
    let old_task = client_id.as_ref().and_then(|id| {
      client_conns
        .borrow()
        .get(id)
        .and_then(|x| conn_tasks.borrow_mut().remove(x))
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let shutdown_ack = Arc::new(tokio::sync::Mutex::new(()));
    let shutdown_ack_guard = Rc::new(shutdown_ack.clone().try_lock_owned().unwrap());
    let concurrency_limiter = concurrency_limiter.clone();

    spawn_local(async move {
      // Fence: old task must be terminated before new task can start
      if let Some(old_task) = old_task {
        drop(old_task.0);
        let _ = old_task.1.lock().await;
        tracing::warn!(?conn.client_id, "aborted previous connection");
      }

      // keep a copy for ourselves
      let _shutdown_ack_guard = shutdown_ack_guard.clone();

      let client_id = conn.client_id.clone();
      with_interrupt(
        shutdown_rx.clone(),
        conn.remote_addr.clone(),
        conn.image_id.clone(),
        "conn_loop",
        None,
        shutdown_ack_guard.clone(),
        conn_loop(
          layered_store,
          config,
          conn,
          shutdown_rx,
          shutdown_ack_guard,
          concurrency_limiter,
        ),
      )
      .await;
      conn_tasks_2.borrow_mut().remove(&conn_id);

      if let Some(client_id) = client_id {
        let mut client_conns = client_conns_2.borrow_mut();
        if client_conns.get(&client_id).copied() == Some(conn_id) {
          client_conns.remove(&client_id);
        }
      }
    });

    conn_tasks
      .borrow_mut()
      .insert(conn_id, (shutdown_tx, shutdown_ack));
    if let Some(client_id) = client_id {
      client_conns.borrow_mut().insert(client_id, conn_id);
    }
  }

  let remaining_tasks = std::mem::replace(&mut *conn_tasks.borrow_mut(), Default::default());

  let (shutdown_tx_it, shutdown_ack_it): (Vec<_>, Vec<_>) = remaining_tasks.into_values().unzip();

  for tx in shutdown_tx_it {
    drop(tx);
  }
  periodic_checkpoint_task.abort();

  for task in shutdown_ack_it {
    let _ = task.lock().await;
  }
  let _ = periodic_checkpoint_task.await;

  let layered_store =
    Rc::into_inner(layered_store).expect("layered_store unique ownership violation");

  let fsync_start = Instant::now();
  if let Err(e) = layered_store.fsync().await {
    tracing::error!(image_id = %config.image_id, error = ?e, "failed to fsync");
  } else {
    tracing::info!(image_id = %config.image_id, duration = ?fsync_start.elapsed(), "fsync completed");
  }

  if let Err(e) = layered_store.request_checkpoint(true).await {
    tracing::error!(image_id = %config.image_id, error = ?e, "failed to request checkpoint");
  }
  if let Err(e) = layered_store.shutdown().await {
    tracing::error!(image_id = %config.image_id, error = ?e, "failed to shutdown layered store");
  }
}

async fn conn_loop(
  layered_store: Rc<LayeredStore>,
  config: Arc<SessionConfig>,
  mut conn: EstablishedConn,
  shutdown_rx: watch::Receiver<()>,
  shutdown_ack_guard: Rc<OwnedMutexGuard<()>>,
  concurrency_limiter: Arc<Semaphore>,
) -> anyhow::Result<()> {
  let wh = Rc::new(tokio::sync::Mutex::new(conn.io_wh));
  let empty_page = Bytes::from(vec![0u8; 1 << conn.page_size_bits]);
  let (buffered_write_tx, buffered_write_rx) = tokio::sync::mpsc::unbounded_channel();
  let write_lock = Arc::new(tokio::sync::Mutex::new(()));

  let write_limiter = Rc::new(RateLimiter::direct(Quota::per_second(
    NonZeroU32::new(50u32).unwrap(),
  )));
  let mut write_is_limited = false;

  spawn_local(with_interrupt(
    shutdown_rx.clone(),
    conn.remote_addr.clone(),
    conn.image_id.clone(),
    "buffered_write_loop",
    Some(wh.clone()),
    shutdown_ack_guard.clone(),
    buffered_write_loop(
      buffered_write_rx,
      layered_store.clone(),
      empty_page.clone(),
      write_lock.clone(),
    ),
  ));

  let mut explicit_txn: Option<Arc<RwLock<Option<(LayeredStoreTxn, OwnedMutexGuard<()>)>>>> = None;

  loop {
    let magic = conn.io_rh.read_u32().await?;
    if magic != 0x25609513 {
      anyhow::bail!("invalid request magic");
    }

    let _cmd_flags = conn.io_rh.read_u16().await?;
    let ty = conn.io_rh.read_u16().await?;
    let cookie = conn.io_rh.read_u64().await?;
    let offset = conn.io_rh.read_u64().await?;
    let length = conn.io_rh.read_u32().await?;

    if length > 1048576 {
      anyhow::bail!("request length too large");
    }

    let Some(linear_end) = offset.checked_add(length as u64) else {
      anyhow::bail!("invalid request range");
    };

    let permit = concurrency_limiter
      .clone()
      .acquire_many_owned(4096 + length)
      .await?;

    let data = if ty == crate::nbd::consts::NBD_CMD_WRITE {
      let mut data = vec![0u8; length as usize];
      conn.io_rh.read_exact(&mut data).await?;
      data
    } else {
      vec![]
    };

    let wh = wh.clone();
    let layered_store = layered_store.clone();
    let empty_page = empty_page.clone();

    if ty == crate::nbd::consts::NBD_CMD_READ {
      let explicit_txn = explicit_txn.clone();
      spawn_local(with_interrupt(
        shutdown_rx.clone(),
        conn.remote_addr.clone(),
        conn.image_id.clone(),
        "async_read",
        Some(wh.clone()),
        shutdown_ack_guard.clone(),
        async move {
          let _permit = permit;
          let txn = match &explicit_txn {
            Some(x) => Either::Left(x.read().await),
            None => Either::Right(layered_store.begin_txn().await?),
          };
          let txn = match &txn {
            Either::Left(x) => match &**x {
              Some(x) => &x.0,
              None => anyhow::bail!("transaction already completed"),
            },
            Either::Right(x) => x,
          };
          let pages = futures::stream::iter(
            IoPlannerIterator::new(1 << conn.page_size_bits, offset..linear_end).map(
              |(page_id, range)| {
                let empty_page = empty_page.clone();
                txn.read_page(page_id).map(move |x| {
                  x.and_then(|x| {
                    let page = x.unwrap_or(empty_page);
                    if page.len() != 1 << conn.page_size_bits {
                      anyhow::bail!(
                        "page size mismatch: page_id={}, actual={}, requested={}",
                        page_id,
                        page.len(),
                        1 << conn.page_size_bits
                      );
                    }
                    Ok(page.slice(range.start as usize..range.end as usize))
                  })
                })
              },
            ),
          )
          .buffered(8)
          .try_collect::<Vec<_>>()
          .await?;
          let mut wh = wh.lock().await;
          wh.write_u32(0x67446698).await?;
          wh.write_u32(0).await?;
          wh.write_u64(cookie).await?;

          for page in pages {
            wh.write_all(&page).await?;
          }
          wh.flush().await?;
          Ok(())
        },
      ));
    } else if ty == crate::nbd::consts::NBD_CMD_WRITE {
      let buffered_write_tx = buffered_write_tx.clone();
      let explicit_txn = explicit_txn.clone();

      if !write_is_limited {
        if rand::thread_rng().gen_range(0..100) == 0 {
          // Throttle if buffer store has too many entries that are not yet checkpointed
          let buffer_store_size = layered_store.buffer_store_size().await?;
          write_is_limited = buffer_store_size >= config.write_throttle_threshold_pages;
          if write_is_limited {
            tracing::warn!(image_id = %conn.image_id, buffer_store_size, "write throttle enabled");
            layered_store.request_checkpoint(false).await?;
          }
        }
      } else {
        let buffer_store_size = layered_store.buffer_store_size().await?;
        write_is_limited = buffer_store_size >= config.write_throttle_threshold_pages;
        if !write_is_limited {
          tracing::warn!(image_id = %conn.image_id, buffer_store_size, "write throttle disabled");
        }
      }

      let write_is_limited = write_is_limited;
      let write_limiter = write_limiter.clone();

      spawn_local(with_interrupt(
        shutdown_rx.clone(),
        conn.remote_addr.clone(),
        conn.image_id.clone(),
        "async_write",
        Some(wh.clone()),
        shutdown_ack_guard.clone(),
        async move {
          let _permit = permit;

          // Throttle?
          if write_is_limited {
            while let Err(_) = write_limiter.check() {
              tokio::time::sleep(Duration::from_millis(
                50 + rand::thread_rng().gen_range(0..50),
              ))
              .await;
            }
          }

          if let Some(txn) = explicit_txn {
            let mut txn = txn.write().await;
            let Some((txn, _)) = &mut *txn else {
              anyhow::bail!("transaction already completed");
            };
            if offset & ((1 << conn.page_size_bits) - 1) != 0
              || (length != (1 << conn.page_size_bits) && length != 0)
            {
              anyhow::bail!(
                "unaligned transactional write, offset={}, length={}",
                offset,
                length
              );
            }
            let page_id = offset >> conn.page_size_bits;
            txn
              .write_page(
                page_id,
                if length == 0 {
                  None
                } else {
                  Some(Bytes::from(data))
                },
              )
              .await?;
          } else {
            let (completion_tx, completion_rx) = oneshot::channel();

            buffered_write_tx
              .send(BufferedWrite {
                linear_range: offset..linear_end,
                data: Bytes::from(data),
                completion_tx,
              })
              .map_err(|_| anyhow::anyhow!("failed to send buffered write"))?;

            // Buffer write failure triggers TCP shutdown, so we don't need to do anything here
            if completion_rx.await.is_err() {
              return Ok(());
            }
          }

          let mut wh = wh.lock().await;
          wh.write_u32(0x67446698).await?;
          wh.write_u32(0).await?;
          wh.write_u64(cookie).await?;
          wh.flush().await?;
          Ok(())
        },
      ));
    } else if ty == crate::nbd::consts::NBD_CMD_MVPS_BEGIN_TXN {
      if explicit_txn.is_some() {
        anyhow::bail!("transaction already started");
      }

      let write_guard = write_lock.clone().lock_owned().await;
      let txn = layered_store.begin_txn().await?;
      explicit_txn = Some(Arc::new(RwLock::new(Some((txn, write_guard)))));

      let mut wh = wh.lock().await;
      wh.write_u32(0x67446698).await?;
      wh.write_u32(0).await?;
      wh.write_u64(cookie).await?;
      wh.flush().await?;
    } else if ty == crate::nbd::consts::NBD_CMD_MVPS_COMMIT_TXN {
      if explicit_txn.is_none() {
        anyhow::bail!("transaction not started");
      }

      let (txn, _write_guard) = explicit_txn
        .take()
        .unwrap()
        .write_owned()
        .await
        .take()
        .unwrap();
      txn.commit().await?;

      let mut wh = wh.lock().await;
      wh.write_u32(0x67446698).await?;
      wh.write_u32(0).await?;
      wh.write_u64(cookie).await?;
      wh.flush().await?;
    } else if ty == crate::nbd::consts::NBD_CMD_MVPS_ROLLBACK_TXN {
      if explicit_txn.is_none() {
        anyhow::bail!("transaction not started");
      }

      let (txn, _write_guard) = explicit_txn
        .take()
        .unwrap()
        .write_owned()
        .await
        .take()
        .unwrap();
      txn.rollback().await?;

      let mut wh = wh.lock().await;
      wh.write_u32(0x67446698).await?;
      wh.write_u32(0).await?;
      wh.write_u64(cookie).await?;
      wh.flush().await?;
    } else if ty == crate::nbd::consts::NBD_CMD_MVPS_LOCK_TXN {
      let Some(txn) = &explicit_txn else {
        anyhow::bail!("transaction not started");
      };
      let mut txn = txn.write().await;
      let txn = &mut txn.as_mut().unwrap().0;
      let locked = txn.lock_for_write().await?;

      let mut wh = wh.lock().await;
      wh.write_u32(0x67446698).await?;
      wh.write_u32(if locked { 0 } else { 1 }).await?;
      wh.write_u64(cookie).await?;
      wh.flush().await?;
    } else {
      anyhow::bail!("unsupported request type: {}", ty);
    }
  }
}

async fn with_interrupt(
  mut interrupt: tokio::sync::watch::Receiver<()>,
  remote: ByteString,
  image_id: ByteString,
  op_name: &str,
  wh: Option<Rc<tokio::sync::Mutex<Pin<Box<dyn AsyncWrite + Send>>>>>,
  _shutdown_ack_guard: Rc<OwnedMutexGuard<()>>,
  fut: impl Future<Output = anyhow::Result<()>>,
) {
  tokio::select! {
    biased;
    _ = interrupt.changed() => {},
    res = fut => {
      match res {
        Ok(()) => {},
        Err(e) => {
          if let Some(e) = e.downcast_ref::<tokio::io::Error>() {
            if e.kind() == tokio::io::ErrorKind::UnexpectedEof || e.kind() == tokio::io::ErrorKind::BrokenPipe {
              return;
            }
          }
          tracing::error!(?remote, %image_id, error = ?e, op_name, "async operation failed");
          if let Some(wh) = wh {
            let _ = wh.lock().await.shutdown().await;
          }
        }
      }
    }
  }
}

struct BufferedWrite {
  linear_range: Range<u64>,
  data: Bytes,
  completion_tx: oneshot::Sender<()>,
}

async fn buffered_write_loop(
  mut rx: UnboundedReceiver<BufferedWrite>,
  layered_store: Rc<LayeredStore>,
  empty_page: Bytes,
  lock: Arc<tokio::sync::Mutex<()>>,
) -> anyhow::Result<()> {
  let mut ticker = tokio::time::interval(Duration::from_millis(5));
  ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
  let page_size = empty_page.len() as u64;

  loop {
    let Some(req) = rx.recv().await else {
      return Ok(());
    };
    let mut req = Some(req);
    ticker.tick().await;

    let mut response_tx_list: Vec<oneshot::Sender<()>> = vec![];
    let mut num_pages: usize = 0;

    let _guard = lock.lock().await;

    let mut txn = layered_store.begin_txn().await?;
    while let Some(mut req) = req.take().or_else(|| rx.try_recv().ok()) {
      for (page_id, range) in IoPlannerIterator::new(page_size, req.linear_range.clone()) {
        let page = if range.start != 0 || range.end != page_size as u64 {
          tracing::warn!(?req.linear_range, page_id, ?range, "slow partial page write");
          let mut page = txn
            .read_page(page_id)
            .await?
            .unwrap_or_else(|| empty_page.clone())
            .to_vec();
          if page.len() as u64 != page_size {
            anyhow::bail!(
              "page size mismatch: page_id={}, actual={}, requested={}",
              page_id,
              page.len(),
              page_size
            );
          }
          page[range.start as usize..range.end as usize]
            .copy_from_slice(&req.data[..range.clone().count()]);
          req.data = req.data.slice(range.count()..);
          Bytes::from(page)
        } else {
          let page = req.data.slice(..page_size as usize);
          req.data = req.data.slice(page_size as usize..);
          page
        };
        txn.write_page(page_id, Some(page)).await?;
        num_pages += 1;
      }
      response_tx_list.push(req.completion_tx);

      if num_pages >= 1000 {
        break;
      }
    }

    txn.commit().await?;

    for tx in response_tx_list {
      let _ = tx.send(());
    }
  }
}
