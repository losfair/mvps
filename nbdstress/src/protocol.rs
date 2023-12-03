use anyhow::Result;
use bytes::Bytes;
use std::{
  cell::RefCell,
  collections::HashMap,
  pin::Pin,
  sync::atomic::{AtomicU64, Ordering},
};
use tokio::{
  io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
  sync::{oneshot, Mutex},
};

// Constants for NBD protocol
const NBD_MAGIC: u64 = 0x4e42444d41474943;
const IHAVEOPT: u64 = 0x49484156454F5054;

const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = NBD_FLAG_FIXED_NEWSTYLE as u32;
const NBD_FLAG_C_NO_ZEROES: u32 = NBD_FLAG_NO_ZEROES as u32;

const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;

const NBD_OPT_EXPORT_NAME: u32 = 1;

// Definition of EstablishedConn
pub struct EstablishedConn {
  pub id: u64,
  pub export_size: u64,
  pub read_half: RefCell<Option<Pin<Box<dyn AsyncRead + Send + 'static>>>>,
  pub write_half: Mutex<Pin<Box<dyn AsyncWrite + Send + 'static>>>,
  pub cookie_to_tx: RefCell<HashMap<u64, (u64, oneshot::Sender<Bytes>)>>,
}

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

pub async fn handshake(
  mut read_half: Pin<Box<dyn AsyncRead + Send + 'static>>,
  mut write_half: Pin<Box<dyn AsyncWrite + Send + 'static>>,
  export_name: &str,
) -> Result<EstablishedConn> {
  // Read and verify server magic number and IHAVEOPT
  let server_magic = read_half.read_u64().await?;
  let ihaveopt = read_half.read_u64().await?;
  if server_magic != NBD_MAGIC || ihaveopt != IHAVEOPT {
    anyhow::bail!("invalid server magic number or IHAVEOPT");
  }

  // Read and handle server handshake flags
  let handshake_flags = read_half.read_u16().await?;

  if handshake_flags & NBD_FLAG_FIXED_NEWSTYLE == 0 {
    anyhow::bail!("server does not support NBD_FLAG_FIXED_NEWSTYLE");
  }

  if handshake_flags & NBD_FLAG_NO_ZEROES == 0 {
    anyhow::bail!("server does not support NBD_FLAG_NO_ZEROES");
  }

  write_half
    .write_u32(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES)
    .await?;

  // Send export name option
  let export_name = export_name.as_bytes();
  write_half.write_u64(IHAVEOPT).await?;
  write_half.write_u32(NBD_OPT_EXPORT_NAME).await?;
  write_half.write_u32(export_name.len() as u32).await?;
  write_half.write_all(export_name).await?;

  write_half.flush().await?;

  let export_size = read_half.read_u64().await?;
  let transmission_flags = read_half.read_u16().await?;

  if transmission_flags & NBD_FLAG_HAS_FLAGS == 0 {
    anyhow::bail!("invalid transmission flags");
  }
  // Construct EstablishedConn with necessary information
  Ok(EstablishedConn {
    id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
    export_size,
    read_half: RefCell::new(Some(read_half)),
    write_half: Mutex::new(write_half),
    cookie_to_tx: RefCell::new(HashMap::new()),
  })
}
