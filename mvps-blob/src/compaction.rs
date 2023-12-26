use std::{collections::BTreeMap, pin::Pin};

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::{
  blob_crypto::CryptoRootKey,
  blob_reader::{BlobReader, PageMetadata},
  blob_writer::{BlobHeaderWriter, BlobHeaderWriterOptions, PageInfoInHeader},
};

struct BlobCursor {
  stream: Pin<Box<dyn Stream<Item = anyhow::Result<BlobChunk>> + Send + 'static>>,
}

#[derive(Clone)]
struct BlobChunk {
  page_id: u64,
  data: Bytes,
}

pub async fn compact_blobs<T: AsRef<BlobReader>>(
  blobs: Vec<T>,
  new_metadata: String,
  root_key: Option<&CryptoRootKey>,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>> {
  assert!(!blobs.is_empty());

  struct IndexEntry {
    blob_seq: usize,
    metadata: PageMetadata,
    overhead: usize,
  }

  let mut page_index: BTreeMap<u64, IndexEntry> = BTreeMap::new();

  for (blob_seq, blob) in blobs.iter().enumerate() {
    let overhead = blob.as_ref().subkey().overhead_bytes();
    for (page_id, page_metadata) in blob.as_ref().page_index() {
      page_index.insert(
        *page_id,
        IndexEntry {
          blob_seq,
          metadata: page_metadata.clone(),
          overhead,
        },
      );
    }
  }

  let mut new_header = BlobHeaderWriter::new(
    BlobHeaderWriterOptions {
      metadata: new_metadata,
    },
    root_key,
  );

  for (page_id, entry) in &page_index {
    if entry.metadata.compressed_size != 0 && entry.metadata.compressed_size < entry.overhead as u64
    {
      anyhow::bail!("page size less than overhead - corruption?");
    }

    let info = PageInfoInHeader {
      id: *page_id,
      compressed_size: if entry.metadata.compressed_size == 0 {
        0
      } else {
        entry.metadata.compressed_size - entry.overhead as u64
          + new_header.subkey().overhead_bytes() as u64
      },
    };

    new_header.add_page(info)?;
  }

  let (new_header, subkey) = new_header.encode();
  let mut cursors = build_cursors(&blobs).await?;

  let stream = async_stream::try_stream! {
    yield new_header;

    for (page_id, entry) in &page_index {
      let cursor = &mut cursors[entry.blob_seq];
      let raw_page = loop {
        let Some(chunk) = cursor.stream.next().await else {
          Err(anyhow::anyhow!("unexpected end of blob stream"))?;
          unreachable!()
        };
        let chunk = chunk?;

        if chunk.page_id == *page_id {
          break chunk.data;
        }
      };

      yield Bytes::from(subkey.encrypt_with_u64_le_nonce(raw_page.to_vec(), *page_id));
    }
  };

  Ok(stream.boxed())
}

async fn build_cursors<T: AsRef<BlobReader>>(blobs: &[T]) -> anyhow::Result<Vec<BlobCursor>> {
  let mut blob_cursors: Vec<BlobCursor> = Vec::with_capacity(blobs.len());

  for blob in blobs {
    blob_cursors.push(BlobCursor {
      stream: blob
        .as_ref()
        .stream_pages()
        .await?
        .map_ok(|(page_id, data)| BlobChunk { page_id, data })
        .boxed(),
    });
  }

  Ok(blob_cursors)
}
