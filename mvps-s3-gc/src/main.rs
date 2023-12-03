use std::collections::HashSet;

use aws_config::{
  retry::RetryConfigBuilder, stalled_stream_protection::StalledStreamProtectionConfig,
};
use aws_sdk_s3::{
  error::SdkError, operation::get_object::GetObjectError, primitives::ByteStream, Client,
};
use bytes::Bytes;
use bytestring::ByteString;
use chrono::{TimeZone, Utc};
use clap::Parser;
use futures::{future::Either, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

#[derive(Deserialize)]
struct MinimalImage {
  #[serde(default)]
  layers: Vec<ByteString>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct GcMark {
  seen_count: u64,
  first_seen_unused_at_ms: i64,
}

enum GcAction {
  Mark(GcMark),
  Delete,
}

/// NBD stress
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  /// S3 bucket
  #[clap(long, env = "MVPS_S3_GC_BUCKET")]
  bucket: String,

  /// S3 prefix
  #[clap(long, default_value = "", env = "MVPS_S3_GC_PREFIX")]
  prefix: String,

  /// Concurrency
  #[clap(long, default_value = "32", env = "MVPS_S3_GC_CONCURRENCY")]
  concurrency: usize,

  /// Number of days before an inactive blob gets deleted
  #[clap(long, default_value = "7", env = "MVPS_S3_GC_THRESHOLD_INACTIVE_DAYS")]
  threshold_inactive_days: u64,

  /// Number of GC runs before an inactive blob gets deleted
  #[clap(long, default_value = "3", env = "MVPS_S3_GC_THRESHOLD_INACTIVE_COUNT")]
  threshold_inactive_count: u64,
}

#[derive(Debug, Clone)]
struct ObjectInfo {
  key: ByteString,
  size: u64,
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

  let s3_client = build_s3_client().await?;

  let images_prefix = format!("{}images/", args.prefix);
  let blobs_prefix = format!("{}blobs/", args.prefix);
  let gcmark_prefix = format!("{}gcmark/", args.prefix);

  let ((blobs, images), gcmarks) = futures::future::try_join(
    async {
      Ok((
        list_objects(&s3_client, &args.bucket, &blobs_prefix).await?,
        list_objects(&s3_client, &args.bucket, &images_prefix).await?,
      ))
    },
    list_objects(&s3_client, &args.bucket, &gcmark_prefix),
  )
  .await?;

  let grouped_images = group_images_by_id(images.iter().map(|x| x.clone()));

  tracing::info!(
    num_images = images.len(),
    num_image_groups = grouped_images.len(),
    num_blobs = blobs.len(),
    num_gcmarks = gcmarks.len(),
    "discovered objects, sleeping 1 second for all TEs to catch up on image versions"
  );
  tokio::time::sleep(std::time::Duration::from_secs(1)).await;

  // Delete old image versions
  let deletion_stream = grouped_images.into_iter().flat_map(|(_, objects)| {
    // Latest object (i.e. the one with the smallest key) must not be deleted
    objects.into_iter().skip(1)
  });

  let deleted_image_keys = futures::stream::iter(deletion_stream)
    .map(|x| {
      let s3_client = s3_client.clone();
      let key = format!("{}{}", images_prefix, x.key);
      async move {
        s3_client
          .delete_object()
          .bucket(&args.bucket)
          .key(key)
          .send()
          .await?;
        Ok::<_, anyhow::Error>(x.key)
      }
    })
    .buffer_unordered(args.concurrency)
    .try_collect::<HashSet<_>>()
    .await?;
  let images = images
    .into_iter()
    .filter(|x| !deleted_image_keys.contains(&x.key))
    .collect::<Vec<_>>();
  tracing::info!(
    num_images = images.len(),
    num_deleted = deleted_image_keys.len(),
    "deleted old images"
  );

  let referenced_blobs = futures::stream::iter(&images)
    .map(|x| {
      list_layers_for_image(
        &s3_client,
        &args.bucket,
        format!("{}{}", images_prefix, x.key),
      )
    })
    .buffer_unordered(args.concurrency)
    .flat_map(|x| match x {
      Ok(x) => Either::Left(futures::stream::iter(x).map(Ok)),
      Err(e) => Either::Right(futures::stream::once(async move { Err(e) })),
    })
    .try_collect::<HashSet<_>>()
    .await?;

  // Sanity check
  if blobs.len() != 0 && images.len() == 0 {
    anyhow::bail!("no images, but there are blobs; not continuing");
  }
  if images.len() != 0 && referenced_blobs.len() == 0 {
    anyhow::bail!("no referenced blobs, but there are images; not continuing");
  }

  let total_blob_size = blobs.iter().map(|x| x.size).sum::<u64>();
  let referenced_blob_size = blobs
    .iter()
    .filter(|x| referenced_blobs.contains(&x.key))
    .map(|x| x.size)
    .sum::<u64>();
  tracing::info!(
    num_referenced_blobs = referenced_blobs.len(),
    total_blob_size = total_blob_size,
    total_blob_size_pretty = humansize::format_size(total_blob_size, humansize::BINARY),
    referenced_blob_size = referenced_blob_size,
    referenced_blob_size_pretty = humansize::format_size(referenced_blob_size, humansize::BINARY),
    "scan completed"
  );

  let max_timestamp_to_delete = Utc::now()
    .checked_sub_signed(chrono::Duration::days(args.threshold_inactive_days as i64))
    .ok_or_else(|| anyhow::anyhow!("failed to calculate max timestamp to delete"))?;

  let num_unmarked_blobs = futures::stream::iter(
    gcmarks
      .iter()
      .filter(|x| referenced_blobs.contains(&x.key))
      .map(|gcmark| async {
        s3_client
          .delete_object()
          .bucket(&args.bucket)
          .key(format!("{}{}", gcmark_prefix, gcmark.key))
          .send()
          .await?;
        Ok::<_, anyhow::Error>(())
      }),
  )
  .buffer_unordered(args.concurrency)
  .try_collect::<Vec<_>>()
  .await?
  .len();

  // Update gcmark or delete object
  let (num_deleted_blobs, num_marked_blobs) = futures::stream::iter(
    blobs
      .iter()
      .filter(|x| !referenced_blobs.contains(&x.key))
      .map(|unused_blob| async {
        let gcmark: GcMark = read_object(
          &s3_client,
          &args.bucket,
          &format!("{}{}", gcmark_prefix, unused_blob.key),
        )
        .await?
        .and_then(|x| match serde_json::from_slice(&x[..]) {
          Ok(x) => Some(x),
          Err(e) => {
            tracing::warn!(error = ?e, key = %unused_blob.key, "failed to parse gcmark");
            None
          }
        })
        .unwrap_or_default();
        let now = Utc::now();
        let first_seen_unused_at = if gcmark.first_seen_unused_at_ms <= 0 {
          None
        } else {
          Utc
            .timestamp_millis_opt(gcmark.first_seen_unused_at_ms)
            .single()
        };
        let action = match (first_seen_unused_at, gcmark.seen_count) {
          (Some(x), n) if x <= max_timestamp_to_delete && n >= args.threshold_inactive_count => {
            GcAction::Delete
          }
          (Some(x), n) => GcAction::Mark(GcMark {
            seen_count: n + 1,
            first_seen_unused_at_ms: x.timestamp_millis(),
          }),
          (None, n) => GcAction::Mark(GcMark {
            seen_count: n + 1,
            first_seen_unused_at_ms: now.timestamp_millis(),
          }),
        };
        Ok::<_, anyhow::Error>((unused_blob.clone(), action))
      }),
  )
  .buffer_unordered(args.concurrency)
  .map_ok(|(unused_blob, action)| {
    let s3_client = &s3_client;
    let blobs_prefix = &blobs_prefix;
    let gcmark_prefix = &gcmark_prefix;
    async move {
      let metrics = match action {
        GcAction::Delete => {
          s3_client
            .delete_object()
            .bucket(&args.bucket)
            .key(format!("{}{}", blobs_prefix, unused_blob.key))
            .send()
            .await?;
          s3_client
            .delete_object()
            .bucket(&args.bucket)
            .key(format!("{}{}", gcmark_prefix, unused_blob.key))
            .send()
            .await?;
          (1u64, 0u64)
        }
        GcAction::Mark(gcmark) => {
          let gcmark = serde_json::to_vec(&gcmark)?;
          s3_client
            .put_object()
            .bucket(&args.bucket)
            .key(format!("{}{}", gcmark_prefix, unused_blob.key))
            .body(ByteStream::from(gcmark))
            .send()
            .await?;
          (0u64, 1u64)
        }
      };
      Ok(metrics)
    }
  })
  .try_buffer_unordered(args.concurrency)
  .try_fold((0u64, 0u64), |(deleted, marked), (d, m)| async move {
    Ok((deleted + d, marked + m))
  })
  .await?;

  tracing::info!(
    num_deleted_blobs = num_deleted_blobs,
    num_marked_blobs = num_marked_blobs,
    num_unmarked_blobs = num_unmarked_blobs,
    "gc completed"
  );

  Ok(())
}

async fn build_s3_client() -> anyhow::Result<Client> {
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
  let s3_client = Client::from_conf(s3_config);

  Ok(s3_client)
}

async fn list_objects(
  client: &Client,
  bucket: &str,
  prefix: &str,
) -> anyhow::Result<Vec<ObjectInfo>> {
  let mut objects = Vec::new();
  let mut continuation_token = None;

  loop {
    let resp = client
      .list_objects_v2()
      .bucket(bucket)
      .prefix(prefix)
      .set_continuation_token(continuation_token)
      .send()
      .await?;
    if let Some(contents) = resp.contents {
      for obj in contents {
        if let Some(key) = &obj.key {
          objects.push(ObjectInfo {
            key: ByteString::from(
              key
                .strip_prefix(&prefix)
                .ok_or_else(|| anyhow::anyhow!("invalid entry in list response"))?,
            ),
            size: obj
              .size()
              .and_then(|x| u64::try_from(x).ok())
              .unwrap_or_default(),
          });
        }
      }
    }
    if let Some(token) = resp.next_continuation_token {
      continuation_token = Some(token);
    } else {
      break;
    }
  }
  Ok(objects)
}

/// Fetch this object from S3, json-parse it as MinimalImage, and return the `layers` field.
/// If the object is not found or json parsing failed, return the empty list
async fn list_layers_for_image(
  client: &Client,
  bucket: &str,
  key: String,
) -> anyhow::Result<Vec<ByteString>> {
  let Some(buf) = read_object(client, bucket, &key).await? else {
    tracing::warn!(key, "image no longer exists");
    return Ok(vec![]);
  };
  let info: MinimalImage = match serde_json::from_slice(&buf) {
    Ok(info) => info,
    Err(e) => {
      tracing::warn!(key, error = ?e, "failed to parse image info");
      return Ok(vec![]);
    }
  };

  Ok(info.layers)
}

async fn read_object(client: &Client, bucket: &str, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
  let buf = loop {
    let resp = client.get_object().bucket(bucket).key(key).send().await;
    if let Err(SdkError::ServiceError(e)) = &resp {
      if matches!(e.err(), GetObjectError::NoSuchKey(_)) {
        return Ok(None);
      }
    }
    let resp = resp?;
    let mut body = resp.body.into_async_read();
    let mut buf = Vec::new();
    if let Err(e) = body.read_to_end(&mut buf).await {
      tracing::warn!(key, error = ?e, "failed to read object, retrying");
      continue;
    }
    break buf;
  };

  Ok(Some(buf))
}

/// Image keys have the format of `image_id/[seq].json`. This method groups all provided
/// image keys by `image_id`.
fn group_images_by_id(images: impl Iterator<Item = ObjectInfo>) -> Vec<(Bytes, Vec<ObjectInfo>)> {
  images
    .filter_map(|obj| {
      let x = obj.key.clone().into_bytes();
      x.iter()
        .find_position(|x| **x == b'/')
        .map(|(index, _)| (x.slice(0..index), obj))
    })
    .group_by(|x| x.0.clone())
    .into_iter()
    .map(|(k, g)| (k, g.map(|x| x.1).collect()))
    .collect::<Vec<_>>()
}
