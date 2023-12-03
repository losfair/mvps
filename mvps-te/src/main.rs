mod bufferstore;
mod caching_image_store;
mod compaction;
mod config;
mod io_planner;
mod layered_store;
mod nbd;
mod session_manager;

#[cfg(test)]
mod tests;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use clap::Parser;
use heed::{flags::Flags, EnvOpenOptions};
use jsonwebtoken::DecodingKey;
use tokio::net::TcpListener;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

use crate::{
  config::{ImageStoreProvider, JitteredInterval, LayeredStoreConfig},
  nbd::{handshake, DeviceOptions},
  session_manager::{SessionManager, SessionManagerConfig},
};

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Multi-versioned page store - transaction engine
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  /// Listen address
  #[clap(long, default_value = "127.0.0.1:10809", env = "MVPS_TE_LISTEN")]
  listen: SocketAddr,

  /// Image store provider. Valid options: local, s3
  #[clap(long, env = "MVPS_TE_IMAGE_STORE")]
  image_store: String,

  /// Image store local path
  #[clap(long, env = "MVPS_TE_IMAGE_STORE_LOCAL_PATH")]
  image_store_local_path: Option<String>,

  /// Image store S3 bucket
  #[clap(long, env = "MVPS_TE_IMAGE_STORE_S3_BUCKET")]
  image_store_s3_bucket: Option<String>,

  /// Image store S3 prefix
  #[clap(long, default_value = "", env = "MVPS_TE_IMAGE_STORE_S3_PREFIX")]
  image_store_s3_prefix: String,

  /// Path to WAL (obsolete, use `buffer_store_path` instead)
  #[clap(long, env = "MVPS_TE_WAL_PATH")]
  wal_path: Option<String>,

  /// Path to buffer store
  #[clap(long, env = "MVPS_TE_BUFFER_STORE_PATH")]
  buffer_store_path: Option<String>,

  /// LMDB max DBs
  #[clap(long, default_value = "16384", env = "MVPS_TE_LMDB_MAX_DBS")]
  lmdb_max_dbs: u32,

  /// LMDB max readers
  #[clap(long, default_value = "2048", env = "MVPS_TE_LMDB_MAX_READERS")]
  lmdb_max_readers: u32,

  /// LMDB map size
  #[clap(
    long,
    default_value = "17179869184",
    env = "MVPS_TE_LMDB_MAP_SIZE_BYTES"
  )]
  lmdb_map_size_bytes: usize,

  /// JWT secret
  #[clap(long, env = "MVPS_TE_JWT_SECRET")]
  jwt_secret: String,

  /// Checkpoint interval
  #[clap(long, default_value = "60000", env = "MVPS_TE_CHECKPOINT_INTERVAL_MS")]
  checkpoint_interval_ms: u64,

  /// Checkpoint interval jitter
  #[clap(
    long,
    default_value = "10000",
    env = "MVPS_TE_CHECKPOINT_INTERVAL_JITTER_MS"
  )]
  checkpoint_interval_jitter_ms: u64,

  /// Image cache size in bytes
  #[clap(
    long,
    default_value = "1073741824",
    env = "MVPS_TE_IMAGE_CACHE_SIZE_BYTES"
  )]
  image_cache_size_bytes: u64,

  /// Image cache block size in bytes
  #[clap(
    long,
    default_value = "131072",
    env = "MVPS_TE_IMAGE_CACHE_BLOCK_SIZE_BYTES"
  )]
  image_cache_block_size_bytes: u64,

  /// Write throttle threshold in number of pages
  #[clap(
    long,
    default_value = "50000",
    env = "MVPS_TE_WRITE_THROTTLE_THRESHOLD_PAGES"
  )]
  write_throttle_threshold_pages: u64,

  /// Disable image store write
  #[clap(long)]
  disable_image_store_write: bool,
}

fn main() -> anyhow::Result<()> {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
  rt.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }

  SubscriberBuilder::default()
    .with_env_filter(EnvFilter::from_default_env())
    .pretty()
    .init();

  let args = Args::parse();

  tracing::info!("starting mvps-te");

  let image_store_provider = match args.image_store.as_str() {
    "local" => ImageStoreProvider::LocalFs {
      path: args
        .image_store_local_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("image store local path is required"))?
        .clone()
        .into(),
    },
    "s3" => ImageStoreProvider::S3 {
      bucket: args
        .image_store_s3_bucket
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("image store S3 bucket is required"))?
        .clone()
        .into(),
      prefix: args.image_store_s3_prefix.clone().into(),
    },
    _ => anyhow::bail!("invalid image store provider"),
  };

  let buffer_store_path = args
    .buffer_store_path
    .as_ref()
    .or_else(|| args.wal_path.as_ref())
    .ok_or_else(|| anyhow::anyhow!("buffer store path is required"))?;

  let bs_env = unsafe {
    EnvOpenOptions::new()
      .max_dbs(args.lmdb_max_dbs)
      .max_readers(args.lmdb_max_readers)
      .map_size(args.lmdb_map_size_bytes)
      .flag(Flags::MdbNoTls)
  }
  .open(buffer_store_path)
  .map_err(|e| {
    anyhow::anyhow!(
      "failed to open buffer store environment at {}: {:?}",
      buffer_store_path,
      e
    )
  })?;
  let nbd_listener = TcpListener::bind(args.listen).await?;
  let session_manager = Arc::new(SessionManager::new(SessionManagerConfig {
    image_store_provider,
    bs_env: Some(bs_env),
    layered_store_config: LayeredStoreConfig {
      disable_image_store_write: args.disable_image_store_write,
      ..LayeredStoreConfig::default()
    },
    checkpoint_interval: JitteredInterval {
      min_ms: args.checkpoint_interval_ms,
      jitter_ms: args.checkpoint_interval_jitter_ms,
    },
    image_cache_size: args.image_cache_size_bytes,
    image_cache_block_size: args.image_cache_block_size_bytes,
    write_throttle_threshold_pages: args.write_throttle_threshold_pages,
  }));
  let jwt_decoding_key: &'static DecodingKey = Box::leak(Box::new(DecodingKey::from_secret(
    args.jwt_secret.as_bytes(),
  )));

  {
    let session_manager = session_manager.clone();
    tokio::spawn(async move {
      loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        session_manager.cleanup();
      }
    });
  }

  loop {
    let (conn, remote) = nbd_listener.accept().await?;
    let (rh, wh) = conn.into_split();
    let rh = Box::pin(tokio::io::BufReader::new(rh));
    let wh = Box::pin(tokio::io::BufWriter::new(wh));
    let session_manager = session_manager.clone();

    tokio::spawn(async move {
      let conn = match handshake(
        rh,
        wh,
        remote,
        |_name| Some(DeviceOptions {}),
        jwt_decoding_key,
      )
      .await
      {
        Ok(x) => x,
        Err(e) => {
          tracing::error!(error = ?e, ?remote, "handshake failed");
          return;
        }
      };
      let session = match session_manager.get_session(conn.image_id.clone()) {
        Ok(x) => x,
        Err(e) => {
          tracing::error!(error = ?e, ?remote, image_id = %conn.image_id, "failed to get session");
          return;
        }
      };

      let image_id = conn.image_id.clone();
      if let Err(e) = session.handover(conn).await {
        tracing::error!(error = ?e, ?remote, %image_id, "handover failed");
      }
    });
  }
}
