mod protocol;

use std::{
  cell::{Cell, RefCell},
  net::SocketAddr,
  rc::Rc,
  time::Duration,
};

use bytes::Bytes;
use clap::Parser;
use futures::future::Either;
use protocol::handshake;
use rand::Rng;
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::TcpStream,
  sync::{oneshot, watch, RwLock},
  task::{spawn_local, JoinHandle},
};
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

use crate::protocol::EstablishedConn;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;

/// NBD stress
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  /// Remote address
  #[clap(long, default_value = "127.0.0.1:10809", env = "NBD_STRESS_REMOTE")]
  remote: SocketAddr,

  /// Export name
  #[clap(long, env = "NBD_STRESS_EXPORT_NAME")]
  export_name: String,

  /// Concurrency
  #[clap(long, default_value = "1000", env = "NBD_STRESS_CONCURRENCY")]
  concurrency: usize,

  /// Block size
  #[clap(long, default_value = "4096", env = "NBD_STRESS_BLOCK_SIZE")]
  block_size: u64,

  /// Percentage of writes, between 0 and 1
  #[clap(long, default_value = "0.5", env = "NBD_STRESS_WRITE_PERCENTAGE")]
  write_percentage: f64,

  /// Reconnect interval in milliseconds. 0 means no reconnect. A 10% jitter is added.
  #[clap(long, default_value = "0", env = "NBD_STRESS_RECONNECT_INTERVAL_MS")]
  reconnect_interval_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct Metrics {
  total_reads: u64,
  validated_reads: u64,
  total_writes: u64,
}

fn main() -> anyhow::Result<()> {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
  let local_set = tokio::task::LocalSet::new();
  local_set.block_on(&rt, async_main())
}

async fn async_main() -> anyhow::Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }

  SubscriberBuilder::default()
    .with_env_filter(EnvFilter::from_default_env())
    .pretty()
    .init();

  let args: &'static Args = Box::leak(Box::new(Args::parse()));

  let (reconnect_tx, mut reconnect_rx) = watch::channel(0u64);
  let reconnect_tx = Rc::new(reconnect_tx);
  let (conn_tx, mut conn_rx) = watch::channel(None::<Rc<EstablishedConn>>);

  let handshake_task = spawn_local(async move {
    let handshake_once = || async {
      let conn = TcpStream::connect(args.remote).await?;
      let (rh, wh) = conn.into_split();
      let rh = tokio::io::BufReader::new(rh);
      let wh = tokio::io::BufWriter::new(wh);
      let conn = handshake(Box::pin(rh), Box::pin(wh), &args.export_name).await?;
      Ok::<_, anyhow::Error>(conn)
    };
    loop {
      let conn = match handshake_once().await {
        Ok(x) => x,
        Err(e) => {
          tracing::error!(error = ?e, "handshake failed");
          tokio::time::sleep(Duration::from_millis(1000)).await;
          continue;
        }
      };
      tracing::info!(conn.export_size, "connection established");
      let id = conn.id;
      conn_tx
        .send(Some(Rc::new(conn)))
        .map_err(|_| ())
        .expect("conn_rx closed");
      while *reconnect_rx.borrow() != id {
        reconnect_rx.changed().await.expect("reconnect_rx closed");
      }
    }
  });

  let conn = conn_rx
    .wait_for(|x| x.is_some())
    .await
    .unwrap()
    .clone()
    .unwrap();
  if conn.export_size % args.block_size != 0 {
    anyhow::bail!("export size is not a multiple of block size");
  }

  let num_blocks = conn.export_size / args.block_size;
  drop(conn);

  let next_cookie: &'static Cell<u64> = Box::leak(Box::new(Cell::new(1)));
  let block_seeds: &'static [RwLock<[u8; 16]>] = (0..num_blocks as usize)
    .map(|_| RwLock::new([0u8; 16]))
    .collect::<Vec<_>>()
    .leak();
  let metrics: &'static RefCell<Metrics> = Box::leak(Box::new(RefCell::new(Metrics {
    total_reads: 0,
    validated_reads: 0,
    total_writes: 0,
  })));

  let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(args.concurrency);

  for _ in 0..args.concurrency {
    let mut conn_rx = conn_rx.clone();
    let reconnect_tx = reconnect_tx.clone();

    let task = spawn_local(async move {
      let mut block_data = vec![0u8; args.block_size as usize];

      loop {
        let should_write = rand::thread_rng().gen::<f64>() < args.write_percentage;
        let block_id = rand::thread_rng().gen_range(0..num_blocks);

        let cookie = next_cookie.get();
        next_cookie.set(cookie + 1);

        let block = &block_seeds[block_id as usize];
        let (_guard, seed) = if should_write {
          let mut g = block.write().await;
          rand::thread_rng().fill(&mut *g);
          let data = *g;
          (Either::Left(g), data)
        } else {
          let g = block.read().await;
          let data = *g;
          (Either::Right(g), data)
        };

        blake3::Hasher::new()
          .update(&seed)
          .finalize_xof()
          .fill(&mut block_data);

        let response = loop {
          let mut conn = Some(conn_rx.borrow().clone().unwrap());
          let conn_id = conn.as_ref().unwrap().id;
          let try_receive_response = async {
            let conn = conn.take().unwrap();
            let (tx, rx) = oneshot::channel::<Bytes>();
            conn.cookie_to_tx.borrow_mut().insert(
              cookie,
              (if !should_write { args.block_size } else { 0 }, tx),
            );

            {
              let mut wh = conn.write_half.lock().await;
              wh.write_u32(0x25609513).await?;
              wh.write_u16(0).await?;
              wh.write_u16(if should_write {
                NBD_CMD_WRITE
              } else {
                NBD_CMD_READ
              })
              .await?;
              wh.write_u64(cookie).await?;
              wh.write_u64(block_id * args.block_size).await?;
              wh.write_u32(args.block_size as u32).await?;

              if should_write {
                wh.write_all(&block_data).await?;
              }

              wh.flush().await?;
            }

            drop(conn);

            Ok::<_, anyhow::Error>(rx.await?)
          };
          match try_receive_response.await {
            Ok(x) => break x,
            Err(_) => {
              //tracing::error!(error = ?e, "failed to receive response, reconnecting");
              reconnect_tx.send_if_modified(|x| {
                if conn_id > *x {
                  *x = conn_id;
                  true
                } else {
                  false
                }
              });
              drop(conn);
              conn_rx.changed().await.unwrap();
              continue;
            }
          }
        };

        {
          let mut metrics = metrics.borrow_mut();
          if !should_write {
            metrics.total_reads += 1;
            if seed != [0u8; 16] {
              if response[..] != block_data[..] {
                panic!("block data mismatch");
              }
              metrics.validated_reads += 1;
            }
          } else {
            metrics.total_writes += 1;
          }
        }
      }
    });
    tasks.push(task);
  }

  let mut conn_rx_2 = conn_rx.clone();
  let reconnect_tx_2 = reconnect_tx.clone();
  let recv_task = spawn_local(async move {
    loop {
      let conn = conn_rx_2.borrow().clone().unwrap();
      tracing::info!(conn_id = conn.id, "recv_task got new connection");
      let mut rh = conn.read_half.borrow_mut().take().unwrap();
      let ret: anyhow::Result<()> = async {
        loop {
          let magic = rh.read_u32().await?;
          assert_eq!(magic, 0x67446698);
          let error = rh.read_u32().await?;
          assert_eq!(error, 0);
          let cookie = rh.read_u64().await?;
          let (num_bytes, tx) = conn
            .cookie_to_tx
            .borrow_mut()
            .remove(&cookie)
            .ok_or_else(|| anyhow::anyhow!("cookie not found"))?;
          let mut bytes = vec![0u8; num_bytes as usize];
          rh.read_exact(&mut bytes).await?;
          let _ = tx.send(Bytes::from(bytes));
        }
      }
      .await;
      tracing::error!(error = ?ret, "recv_task failed, reconnecting");
      reconnect_tx_2.send_if_modified(|x| {
        if conn.id > *x {
          *x = conn.id;
          true
        } else {
          false
        }
      });
      drop(conn);
      conn_rx_2.changed().await.unwrap();
    }
  });
  tasks.push(recv_task);

  tasks.push(handshake_task);

  let conn_rx_2 = conn_rx.clone();
  let reconnect_tx_2 = reconnect_tx.clone();
  let trigger_reconnect_task = spawn_local(async move {
    if args.reconnect_interval_ms == 0 {
      return futures::future::pending().await;
    }

    loop {
      let interval = Duration::from_millis(
        args.reconnect_interval_ms
          + rand::thread_rng().gen_range(0..args.reconnect_interval_ms / 10),
      );
      tokio::time::sleep(interval).await;
      let conn = conn_rx_2.borrow().clone().unwrap();
      reconnect_tx_2.send_if_modified(|x| {
        if conn.id > *x {
          *x = conn.id;
          true
        } else {
          false
        }
      });
    }
  });
  tasks.push(trigger_reconnect_task);

  spawn_local(async move {
    let ret = futures::future::select_all(tasks).await.0;
    tracing::error!(error = ?ret, "task failed");
    std::process::abort();
  });

  let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
  let mut prev_metrics = Metrics::default();
  loop {
    interval.tick().await;
    let metrics = metrics.borrow().clone();
    tracing::info!(
      total_reads = metrics.total_reads,
      total_validated_reads = metrics.validated_reads,
      total_writes = metrics.total_writes,
      new_reads = metrics.total_reads - prev_metrics.total_reads,
      new_validated_reads = metrics.validated_reads - prev_metrics.validated_reads,
      new_writes = metrics.total_writes - prev_metrics.total_writes,
      "metrics"
    );
    prev_metrics = metrics;
  }
}
