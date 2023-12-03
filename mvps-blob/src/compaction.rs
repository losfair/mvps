use std::{collections::BTreeMap, pin::Pin};

use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::{
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
) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>> {
  assert!(!blobs.is_empty());

  let mut page_index: BTreeMap<u64, (usize, PageMetadata)> = BTreeMap::new();

  for (blob_seq, blob) in blobs.iter().enumerate() {
    for (page_id, page_metadata) in blob.as_ref().page_index() {
      page_index.insert(*page_id, (blob_seq, page_metadata.clone()));
    }
  }

  let mut new_header = BlobHeaderWriter::new(BlobHeaderWriterOptions {
    metadata: new_metadata,
  });

  for (page_id, (_, page_metadata)) in &page_index {
    new_header.add_page(PageInfoInHeader {
      id: *page_id,
      compressed_size: page_metadata.compressed_size,
    })?;
  }

  let new_header = new_header.encode();
  let mut cursors = build_cursors(&blobs).await?;

  let stream = async_stream::try_stream! {
    yield new_header;

    for (page_id, (blob_seq, _)) in &page_index {
      let cursor = &mut cursors[*blob_seq];
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

      yield raw_page;
    }
  };

  Ok(stream.boxed())
}

async fn build_cursors<T: AsRef<BlobReader>>(blobs: &[T]) -> anyhow::Result<Vec<BlobCursor>> {
  let mut blob_cursors: Vec<BlobCursor> = Vec::with_capacity(blobs.len());

  for blob in blobs {
    let mut page_sizes: Vec<u64> = Vec::with_capacity(blob.as_ref().page_index().len());
    let mut page_ids: Vec<u64> = Vec::with_capacity(blob.as_ref().page_index().len());

    for (page_id, page_metadata) in blob.as_ref().page_index() {
      page_sizes.push(page_metadata.compressed_size);
      page_ids.push(*page_id);
    }

    blob_cursors.push(BlobCursor {
      stream: blob
        .as_ref()
        .backend()
        .clone()
        .stream_chunks(blob.as_ref().body_offset(), page_sizes)
        .await?
        .zip(futures::stream::iter(page_ids.into_iter()))
        .map(|(data, page_id)| data.map(|data| BlobChunk { page_id, data }))
        .boxed(),
    });
  }

  Ok(blob_cursors)
}
