use std::{net::SocketAddr, pin::Pin};

use bytestring::ByteString;
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub struct EstablishedConn {
  pub io_rh: Pin<Box<dyn AsyncRead + Send + 'static>>,
  pub io_wh: Pin<Box<dyn AsyncWrite + Send + 'static>>,
  pub remote_addr: SocketAddr,
  pub image_id: ByteString,
  pub client_id: Option<ByteString>,
  pub page_size_bits: u32,
}

#[derive(Debug, Clone)]
pub struct DeviceOptions {}

#[derive(Deserialize)]
struct HandshakeClaims {
  #[serde(default)]
  client_id: Option<ByteString>,
  image_id: ByteString,
  image_size: u64,
  page_size_bits: u32,
}

pub async fn handshake(
  mut rh: Pin<Box<dyn AsyncRead + Send + 'static>>,
  mut wh: Pin<Box<dyn AsyncWrite + Send + 'static>>,
  remote_addr: SocketAddr,
  get_options: impl FnOnce(ByteString) -> Option<DeviceOptions>,
  jwt_decoding_key: &DecodingKey,
) -> anyhow::Result<EstablishedConn> {
  wh.write_all(b"NBDMAGIC").await?;
  wh.write_all(b"IHAVEOPT").await?;

  let flags: u16 = consts::NBD_FLAG_NO_ZEROES | consts::NBD_FLAG_FIXED_NEWSTYLE;
  wh.write_u16(flags).await?;

  wh.flush().await?;

  let client_flags = rh.read_u32().await?;
  if client_flags & consts::NBD_FLAG_C_NO_ZEROES == 0 {
    anyhow::bail!("client does not support NBD_FLAG_NO_ZEROES");
  }
  if client_flags & consts::NBD_FLAG_C_FIXED_NEWSTYLE == 0 {
    anyhow::bail!("client does not support NBD_FLAG_C_FIXED_NEWSTYLE");
  }
  if client_flags & !(consts::NBD_FLAG_C_NO_ZEROES | consts::NBD_FLAG_C_FIXED_NEWSTYLE) != 0 {
    anyhow::bail!("client sent unknown flags: {:08x}", client_flags);
  }

  let export_name: String;

  loop {
    let mut ihaveopt = [0u8; 8];
    rh.read_exact(&mut ihaveopt).await?;
    if &ihaveopt[..] != b"IHAVEOPT" {
      anyhow::bail!("expected IHAVEOPT, got {:?}", ihaveopt);
    }

    let option = rh.read_u32().await?;
    let option_data_len = rh.read_u32().await?;
    if option_data_len > 65536 {
      anyhow::bail!("option data too long: {}", option_data_len);
    }
    let mut option_data = vec![0u8; option_data_len as usize];
    rh.read_exact(&mut option_data).await?;

    if option == consts::NBD_OPT_EXPORT_NAME {
      export_name = String::from_utf8(option_data)?;
      break;
    }

    wh.write_u64(0x3e889045565a9).await?;
    wh.write_u32(option).await?;

    if option == consts::NBD_OPT_ABORT {
      wh.write_u32(consts::NBD_REP_ACK).await?;
      wh.write_u32(0).await?;
    } else {
      tracing::warn!(option, "unsupported nbd option during handshake");
      wh.write_u32(consts::NBD_REP_ERR_UNSUP).await?;
      wh.write_u32(0).await?;
    }

    wh.flush().await?;
  }

  let token = jsonwebtoken::decode::<HandshakeClaims>(
    &export_name,
    jwt_decoding_key,
    &jsonwebtoken::Validation::default(),
  )?;

  let _export =
    get_options(token.claims.image_id.clone()).ok_or_else(|| anyhow::anyhow!("no such export"))?;
  wh.write_u64(token.claims.image_size).await?;

  let flags = consts::NBD_FLAG_HAS_FLAGS;
  wh.write_u16(flags).await?;
  wh.flush().await?;

  Ok(EstablishedConn {
    io_rh: rh,
    io_wh: wh,
    remote_addr,
    image_id: token.claims.image_id,
    client_id: token.claims.client_id,
    page_size_bits: token.claims.page_size_bits,
  })
}

#[allow(dead_code)]
pub mod consts {
  pub const NBD_OPT_EXPORT_NAME: u32 = 1;
  pub const NBD_OPT_ABORT: u32 = 2;
  pub const NBD_OPT_LIST: u32 = 3;
  pub const NBD_OPT_STARTTLS: u32 = 5;
  pub const NBD_OPT_INFO: u32 = 6;
  pub const NBD_OPT_GO: u32 = 7;
  pub const NBD_OPT_STRUCTURED_REPLY: u32 = 8;

  pub const NBD_REP_ACK: u32 = 1;
  pub const NBD_REP_SERVER: u32 = 2;
  pub const NBD_REP_INFO: u32 = 3;
  pub const NBD_REP_FLAG_ERROR: u32 = 1 << 31;
  pub const NBD_REP_ERR_UNSUP: u32 = 1 | NBD_REP_FLAG_ERROR;
  pub const NBD_REP_ERR_POLICY: u32 = 2 | NBD_REP_FLAG_ERROR;
  pub const NBD_REP_ERR_INVALID: u32 = 3 | NBD_REP_FLAG_ERROR;
  pub const NBD_REP_ERR_PLATFORM: u32 = 4 | NBD_REP_FLAG_ERROR;
  pub const NBD_REP_ERR_TLS_REQD: u32 = 5 | NBD_REP_FLAG_ERROR;
  pub const NBD_REP_ERR_UNKNOWN: u32 = 6 | NBD_REP_FLAG_ERROR;
  pub const NBD_REP_ERR_BLOCK_SIZE_REQD: u32 = 8 | NBD_REP_FLAG_ERROR;

  pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
  pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

  pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = NBD_FLAG_FIXED_NEWSTYLE as u32;
  pub const NBD_FLAG_C_NO_ZEROES: u32 = NBD_FLAG_NO_ZEROES as u32;

  pub const NBD_INFO_EXPORT: u16 = 0;
  pub const NBD_INFO_NAME: u16 = 1;
  pub const NBD_INFO_DESCRIPTION: u16 = 2;
  pub const NBD_INFO_BLOCK_SIZE: u16 = 3;

  pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
  pub const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
  pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
  pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
  pub const NBD_FLAG_ROTATIONAL: u16 = 1 << 4;
  pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
  pub const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
  pub const NBD_FLAG_CAN_MULTI_CONN: u16 = 1 << 8;
  pub const NBD_FLAG_SEND_RESIZE: u16 = 1 << 9;

  pub const NBD_CMD_READ: u16 = 0;
  pub const NBD_CMD_WRITE: u16 = 1;
  pub const NBD_CMD_DISC: u16 = 2;
  pub const NBD_CMD_FLUSH: u16 = 3;
  pub const NBD_CMD_TRIM: u16 = 4;
  pub const NBD_CMD_WRITE_ZEROES: u16 = 6;
  pub const NBD_CMD_RESIZE: u16 = 8;

  pub const NBD_CMD_MVPS_BEGIN_TXN: u16 = 64;
  pub const NBD_CMD_MVPS_COMMIT_TXN: u16 = 65;
  pub const NBD_CMD_MVPS_ROLLBACK_TXN: u16 = 66;
  pub const NBD_CMD_MVPS_LOCK_TXN: u16 = 67;
}
