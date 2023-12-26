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

use std::{
  pin::Pin,
  str::FromStr,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use anyhow::Context;
use bytestring::ByteString;
use clap::Parser;
use futures::future::Either;
use heed::{flags::Flags, EnvOpenOptions};
use jsonwebtoken::DecodingKey;
use mvps_blob::blob_crypto::{CryptoRootKey, SubkeyAlgorithm};
use tokio::{
  io::{AsyncRead, AsyncWrite},
  net::{TcpListener, UnixListener},
  signal::unix::{signal, SignalKind},
};
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
  listen: String,

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

  /// LMDB NOSYNC flag
  #[clap(long)]
  lmdb_nosync: bool,

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

  /// Root key
  #[clap(long, env = "MVPS_TE_ROOT_KEY")]
  root_key: Option<String>,

  /// Comma-separated list of decryption keys (in addition to the root key)
  #[clap(
    long,
    env = "MVPS_TE_DECRYPTION_KEYS",
    use_value_delimiter = true,
    value_delimiter = ','
  )]
  decryption_keys: Vec<String>,

  #[clap(
    long,
    env = "MVPS_TE_SUBKEY_ALGORITHM",
    default_value = "chacha20poly1305"
  )]
  subkey_algorithm: String,
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
    let mut opts = EnvOpenOptions::new();
    opts
      .max_dbs(args.lmdb_max_dbs)
      .max_readers(args.lmdb_max_readers)
      .map_size(args.lmdb_map_size_bytes)
      .flag(Flags::MdbNoTls);
    if args.lmdb_nosync {
      opts.flag(Flags::MdbNoSync);
    }
    opts
  }
  .open(buffer_store_path)
  .map_err(|e| {
    anyhow::anyhow!(
      "failed to open buffer store environment at {}: {:?}",
      buffer_store_path,
      e
    )
  })?;
  let nbd_listener = if let Some(path) = args.listen.strip_prefix("unix:") {
    let _ = std::fs::remove_file(path);
    Either::Left(UnixListener::bind(path)?)
  } else {
    Either::Right(TcpListener::bind(&args.listen).await?)
  };

  let root_key: Arc<Option<CryptoRootKey>>;
  let mut decryption_keys: Vec<CryptoRootKey> = vec![];
  let subkey_algorithm = SubkeyAlgorithm::from_str(&args.subkey_algorithm)
    .map_err(|_| anyhow::anyhow!("invalid subkey algorithm: {}", args.subkey_algorithm))?;

  if let Some(x) = &args.root_key {
    root_key = Arc::new(Some(
      CryptoRootKey::new(x, subkey_algorithm).with_context(|| "invalid root key")?,
    ));
    decryption_keys.push(CryptoRootKey::new(x, subkey_algorithm).unwrap());
    tracing::info!(
      "loaded root key with subkey algorithm {:?}",
      subkey_algorithm
    );
  } else {
    root_key = Arc::new(None);
  };

  for (i, decryption_key) in args.decryption_keys.iter().enumerate() {
    decryption_keys.push(
      CryptoRootKey::new(decryption_key, subkey_algorithm)
        .with_context(|| format!("invalid decryption key at index {}", i))?,
    );
  }
  if !decryption_keys.is_empty() {
    tracing::info!(num_keys = decryption_keys.len(), "loaded decryption keys");
  }

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
    root_key,
    decryption_keys: Arc::new(decryption_keys),
  }));
  let jwt_decoding_key: &'static DecodingKey = Box::leak(Box::new(DecodingKey::from_secret(
    args.jwt_secret.as_bytes(),
  )));

  // Signal handling & periodic cleanup
  let is_shutting_down = Arc::new(AtomicBool::new(false));
  {
    let session_manager = session_manager.clone();
    let mut stream = signal(SignalKind::terminate()).unwrap();
    let is_shutting_down = is_shutting_down.clone();
    std::thread::Builder::new()
      .name("mvps-cleanup".into())
      .spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
          .enable_all()
          .build()
          .unwrap();
        rt.block_on(async {
          loop {
            tokio::select! {
              _ = tokio::time::sleep(Duration::from_secs(5)) => {
                session_manager.cleanup();
              }
              _ = stream.recv() => {
                tracing::info!("shutting down");
                is_shutting_down.store(true, Ordering::Relaxed);
                session_manager.shutdown_blocking();
                std::process::exit(0);
              }
            }
          }
        })
      })
      .unwrap();
  }

  loop {
    let (rh, wh, remote): (
      Pin<Box<dyn AsyncRead + Send>>,
      Pin<Box<dyn AsyncWrite + Send>>,
      String,
    ) = match &nbd_listener {
      Either::Left(l) => {
        let (conn, remote) = l.accept().await?;
        let (rh, wh) = conn.into_split();
        (
          Box::pin(tokio::io::BufReader::new(rh)),
          Box::pin(tokio::io::BufWriter::new(wh)),
          format!(
            "unix:{}",
            remote
              .as_pathname()
              .and_then(|x| x.to_str())
              .unwrap_or_default()
          ),
        )
      }
      Either::Right(l) => {
        let (conn, remote) = l.accept().await?;
        conn.set_nodelay(true)?;
        let (rh, wh) = conn.into_split();
        (
          Box::pin(tokio::io::BufReader::new(rh)),
          Box::pin(tokio::io::BufWriter::new(wh)),
          remote.to_string(),
        )
      }
    };
    let remote = ByteString::from(remote);
    let session_manager = session_manager.clone();

    if is_shutting_down.load(Ordering::Relaxed) {
      continue;
    }

    tokio::spawn(async move {
      let conn = match handshake(
        rh,
        wh,
        remote.clone(),
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
