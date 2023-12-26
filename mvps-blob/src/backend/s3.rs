use crate::interfaces::{ImageInfo, ImageStore, RemoteBlob, RemoteBlobMetadata};
use async_trait::async_trait;
use aws_config::{
  retry::RetryConfigBuilder, stalled_stream_protection::StalledStreamProtectionConfig,
};
use aws_sdk_s3::{
  operation::head_object::HeadObjectOutput,
  primitives::ByteStream,
  types::{CompletedMultipartUpload, CompletedPart},
  Client as S3Client,
};
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use chrono::{TimeZone, Utc};
use futures::{future::Either, FutureExt, Stream, StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use std::{ops::Range, pin::Pin, rc::Rc};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

static S3_WORKER: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
  tokio::runtime::Builder::new_multi_thread()
    .worker_threads(1)
    .max_blocking_threads(1)
    .thread_name("s3-worker")
    .enable_all()
    .build()
    .unwrap()
});

pub struct S3ImageStore {
  s3_client: S3Client,
  bucket: ByteString,
  prefix: ByteString,
}

impl S3ImageStore {
  pub async fn new(bucket: ByteString, prefix: ByteString) -> Self {
    let mut builder = aws_config::load_from_env()
      .await
      .into_builder()
      .retry_config(
        RetryConfigBuilder::new()
          .max_attempts(std::u32::MAX)
          .build(),
      );

    let mut path_style = false;
    if let Ok(endpoint_url) = std::env::var("S3_ENDPOINT") {
      path_style = true;
      builder.set_endpoint_url(Some(endpoint_url));
    }

    let shared_config = builder.build();
    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
      .force_path_style(path_style)
      .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
      .build();
    let s3_client = S3Client::from_conf(s3_config);

    // Trigger lazy construction
    let _: &tokio::runtime::Runtime = &S3_WORKER;

    Self {
      s3_client,
      bucket,
      prefix,
    }
  }
}

#[async_trait(?Send)]
impl ImageStore for S3ImageStore {
  async fn get_image_info(&self, image_id: &str) -> anyhow::Result<ImageInfo> {
    let resp = self
      .s3_client
      .list_objects_v2()
      .bucket(&*self.bucket)
      .prefix(format!("{}images/{}/", self.prefix, image_id))
      .send()
      .await?;

    // return first object
    let object = resp.contents.and_then(|x| x.into_iter().next());
    let Some(object) = object else {
      return Ok(ImageInfo {
        version: 1,
        change_count: 0,
        writer_id: None,
        layers: vec![],
      });
    };

    let body = loop {
      let resp = self
        .s3_client
        .get_object()
        .bucket(&*self.bucket)
        .key(object.key.as_deref().unwrap_or_default())
        .send()
        .await?;

      match resp.body.collect().await {
        Ok(x) => break x.into_bytes(),
        Err(e) => {
          tracing::error!(error = ?e, "get_image_info failed to receive streaming body from s3, retrying");
        }
      }
    };

    let image_info: ImageInfo = serde_json::from_slice(&body)?;
    Ok(image_info)
  }

  async fn set_image_info(&self, image_id: &str, info: &ImageInfo) -> anyhow::Result<()> {
    let change_count_suffix = hex::encode((std::u64::MAX - info.change_count).to_be_bytes());
    let serialized_info = serde_json::to_vec(info)?;
    self
      .s3_client
      .put_object()
      .bucket(&*self.bucket)
      .key(format!(
        "{}images/{}/{}-{}.json",
        self.prefix,
        image_id,
        change_count_suffix,
        info.writer_id.as_deref().unwrap_or_default()
      ))
      .body(ByteStream::from(serialized_info))
      .send()
      .await?;
    Ok(())
  }

  async fn get_blob(self: Rc<Self>, blob_id: &str) -> anyhow::Result<Rc<dyn RemoteBlob>> {
    let key = format!("{}blobs/{}", self.prefix, blob_id);
    let metadata = self
      .s3_client
      .head_object()
      .bucket(&*self.bucket)
      .key(&key)
      .send()
      .await?;
    Ok(Rc::new(S3Blob {
      s3_client: self.s3_client.clone(),
      bucket: self.bucket.clone(),
      key: key.into(),
      metadata,
    }))
  }

  async fn set_blob(
    &self,
    blob_id: &str,
    blob_stream: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>,
  ) -> anyhow::Result<()> {
    const PART_SIZE: usize = 16 * 1024 * 1024;

    let client = self.s3_client.clone();
    let bucket = self.bucket.clone();
    let key = format!("{}blobs/{}", self.prefix, blob_id);

    let (work, work_ret) = async move {
      // Start multipart upload
      let create_resp = client
        .create_multipart_upload()
        .bucket(&*bucket)
        .key(&key)
        .send()
        .await?;

      let upload_id = create_resp
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("Failed to get upload ID"))?;

      let chunks = blob_stream
        .flat_map(|x| match x {
          Ok(x) => Either::Left(futures::stream::iter(x.into_iter().map(Ok))),
          Err(e) => Either::Right(futures::stream::once(futures::future::ready(Err(e)))),
        })
        .try_chunks(PART_SIZE);

      let completed_parts = chunks
        .enumerate()
        .map(|(i, x)| x.map(|x| (i + 1, x)))
        .map_err(anyhow::Error::from)
        .map_ok(|(part_number, chunk)| {
          let client = &client;
          let bucket = &bucket;
          let key = &key;
          async move {
            let resp = client
              .upload_part()
              .bucket(&**bucket)
              .key(key)
              .upload_id(upload_id)
              .part_number(part_number as i32)
              .body(ByteStream::from(chunk))
              .send()
              .await?;
            let Some(etag) = resp.e_tag else {
              anyhow::bail!("Failed to get etag");
            };

            Ok::<_, anyhow::Error>(
              CompletedPart::builder()
                .e_tag(etag)
                .part_number(part_number as i32)
                .build(),
            )
          }
        })
        .try_buffered(4)
        .try_collect::<Vec<_>>()
        .await?;

      // Complete the multipart upload
      client
        .complete_multipart_upload()
        .bucket(&*bucket)
        .key(&key)
        .upload_id(upload_id)
        .multipart_upload(
          CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build(),
        )
        .send()
        .await?;
      Ok::<_, anyhow::Error>(())
    }
    .remote_handle();

    S3_WORKER.spawn(work);
    work_ret.await?;

    Ok(())
  }
}

pub struct S3Blob {
  s3_client: S3Client,
  bucket: ByteString,
  key: ByteString,
  metadata: HeadObjectOutput,
}

#[async_trait(?Send)]
impl RemoteBlob for S3Blob {
  async fn read_metadata(&self) -> anyhow::Result<RemoteBlobMetadata> {
    Ok(RemoteBlobMetadata {
      created_at: Utc
        .timestamp_millis_opt(
          self
            .metadata
            .last_modified()
            .ok_or_else(|| anyhow::anyhow!("Failed to get last modified time"))?
            .to_millis()?,
        )
        .single()
        .ok_or_else(|| anyhow::anyhow!("Failed to parse last modified time"))?,
    })
  }

  async fn read_range(&self, file_offset_range: Range<u64>) -> anyhow::Result<Vec<u8>> {
    let requested_len = file_offset_range.end - file_offset_range.start;
    if requested_len == 0 {
      return Ok(vec![]);
    }

    let fetch_len = file_offset_range
      .end
      .min(self.metadata.content_length().unwrap_or(0) as u64)
      .saturating_sub(file_offset_range.start);

    let mut body = loop {
      let resp = self
        .s3_client
        .get_object()
        .bucket(&*self.bucket)
        .key(&*self.key)
        .range(format!(
          "bytes={}-{}",
          file_offset_range.start,
          (file_offset_range.start + fetch_len).saturating_sub(1),
        ))
        .send()
        .await?;

      match resp.body.collect().await {
        Ok(x) => break x.to_vec(),
        Err(e) => {
          tracing::error!(error = ?e, "read_range failed to receive streaming body from s3, retrying");
        }
      }
    };
    if body.len() as u64 != fetch_len {
      anyhow::bail!("received bad range from s3");
    }

    if fetch_len < requested_len {
      // pad with zeros
      body.resize(requested_len as usize, 0);
    }
    Ok(body)
  }

  async fn stream_raw_chunks(
    self: Rc<Self>,
    file_offset_start: u64,
    chunk_sizes: Vec<u64>,
  ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(4);
    let bucket = self.bucket.clone();
    let blob_key = self.key.clone();
    let client = self.s3_client.clone();

    let (work, work_ret) = async move {
      let mut current_offset = file_offset_start;

      loop {
        let resp = match client
          .get_object()
          .bucket(&*bucket)
          .key(&*blob_key)
          .range(format!("bytes={}-", current_offset))
          .send()
          .await
        {
          Ok(x) => x,
          Err(e) => {
            tracing::error!(error = ?e, %blob_key, "stream_chunks: failed to get object");
            return;
          }
        };
        let mut body = resp.body.into_async_read();

        loop {
          let mut buf = BytesMut::with_capacity(32768);
          match body.read_buf(&mut buf).await {
            Ok(0) => {
              return;
            }
            Ok(n) => assert_eq!(n, buf.len()),
            Err(e) => {
              tracing::error!(%blob_key, error = ?e, "stream_chunks: chunk read failed, reconnecting");
              break;
            }
          };
          current_offset += buf.len() as u64;

          if tx.send(buf.freeze()).await.is_err() {
            return;
          }
        }
      }
    }
    .remote_handle();

    S3_WORKER.spawn(work);
    let mut rx = StreamReader::new(ReceiverStream::new(rx).map(Ok::<_, std::io::Error>));

    let stream = async_stream::try_stream! {
      for chunk_size in chunk_sizes {
        let mut buf = vec![0; chunk_size as usize];
        rx.read_exact(&mut buf).await?;
        yield Bytes::from(buf);
      }
      drop(work_ret);
    };
    Ok(Box::pin(stream))
  }
}
