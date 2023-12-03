use std::{collections::HashMap, iter::once, ops::Range, rc::Rc};

use anyhow::Context;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use mvps_blob::{
  blob_reader::{BlobReader, PageMetadata},
  blob_writer::{BlobHeaderWriter, BlobHeaderWriterOptions, PageInfoInHeader},
  compaction::compact_blobs,
  image::{ImageManager, Layer},
  interfaces::ImageStore,
  util::generate_blob_id,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::config::CompactionThreshold;

#[derive(Debug)]
struct Candidate {
  continous_from: usize,
  total_size: u64,
}

pub async fn trim_and_compact_once(
  mut with_im: impl FnMut(&mut dyn FnMut(&mut ImageManager)),
  mut request_writeback: impl FnMut(),
  thresholds: &[CompactionThreshold],
) {
  let mut image_id = None;
  let mut image_store: Option<Rc<dyn ImageStore>> = None;
  with_im(&mut |im| {
    image_id = Some(im.image_id.clone());
    image_store = Some(im.store.clone());
  });
  let image_id = image_id.unwrap();
  let image_store = image_store.unwrap();

  // Trim before compact
  for trim_iteration in 0u64..64u64 {
    let mut layers: Vec<Rc<Layer>> = vec![];
    with_im(&mut |im| {
      layers = im.layers.clone();
    });

    let old_layers = layers;
    let mut num_trimmed_bytes: u64 = 0;
    let mut new_layers = trim_once(
      old_layers.clone(),
      image_store.clone(),
      &mut num_trimmed_bytes,
    )
    .await;

    if num_trimmed_bytes == 0 {
      break;
    }

    with_im(&mut |im| {
      for (old, current) in old_layers.iter().zip(im.layers.iter()) {
        assert_eq!(old.blob_id, current.blob_id);
      }

      let current_layers = std::mem::replace(&mut im.layers, vec![]);
      let new_layers = std::mem::replace(&mut new_layers, vec![]);
      im.layers = new_layers
        .into_iter()
        .chain(current_layers.into_iter().skip(old_layers.len()))
        .collect();
    });
    request_writeback();
    tracing::info!(%image_id, trim_iteration, num_trimmed_bytes, num_trimmed_bytes_pretty = humansize::format_size(num_trimmed_bytes, humansize::BINARY), "trimmed image");
  }

  // Now, do the compaction
  for (level, threshold) in thresholds.iter().enumerate() {
    let mut layers: Vec<Rc<Layer>> = vec![];
    with_im(&mut |im| {
      layers = im.layers.clone();
    });

    if (layers.len() as u64) < threshold.pressure_level {
      tracing::debug!(
        %image_id,
        level,
        num_layers = layers.len(),
        threshold_pressure_level = threshold.pressure_level,
        "not performing compaction because required pressure level is not reached"
      );
      continue;
    }

    let mut ranges: Vec<Range<usize>> = vec![];

    let mut candidate: Option<Candidate> = None;
    for (i, layer) in layers.iter().map(Some).chain(once(None)).enumerate() {
      //tracing::info!(i, size = ?layer.as_ref().map(|x| x.blob.size()), ?candidate, "iterating");
      let layer_exceeds_limit =
        layer.is_some() && layer.unwrap().blob.size() > threshold.max_input_size;
      let candidate_exceeds_limit = candidate
        .as_ref()
        .map(|x| {
          x.total_size + layer.map(|x| x.blob.size()).unwrap_or_default()
            >= threshold.max_output_size
        })
        .unwrap_or_default();

      if candidate_exceeds_limit || layer_exceeds_limit || layer.is_none() {
        // terminate current sequence
        let candidate = candidate
          .take()
          .filter(|x| x.continous_from != i - 1 && x.total_size >= threshold.min_output_size);
        if let Some(candidate) = candidate {
          ranges.push(candidate.continous_from..i);
        }
      }

      let Some(layer) = layer else {
        continue;
      };

      if layer_exceeds_limit {
        candidate = None;
      } else if candidate.is_none() {
        candidate = Some(Candidate {
          continous_from: i,
          total_size: layer.blob.size(),
        });
      } else {
        candidate.as_mut().unwrap().total_size += layer.blob.size();
      }
    }
    drop(candidate);

    if ranges.is_empty() {
      continue;
    }

    let mut old_layers_offset: usize = 0;

    for range in ranges {
      let blob_ids = layers[range.clone()]
        .iter()
        .map(|x| &x.blob_id)
        .collect::<Vec<_>>();
      tracing::info!(level, ?range, ?blob_ids, "compacting range");
      let blobs = layers[range.clone()]
        .iter()
        .map(|x| &x.blob)
        .collect::<Vec<_>>();
      let new_layer = match compact_and_persist_layer(image_store.clone(), blobs).await {
        Ok(x) => Rc::new(x),
        Err(e) => {
          tracing::error!(%image_id, error = ?e, ?range, ?blob_ids, "failed to compact and persist range");
          continue;
        }
      };

      with_im(&mut |im| {
        assert!((old_layers_offset == 0 && range.start == 0) || range.start > old_layers_offset);
        let range = range.start - old_layers_offset..range.end - old_layers_offset;
        for i in range.clone() {
          assert_eq!(im.layers[i].blob_id, layers[i + old_layers_offset].blob_id);
        }
        let new_layers = Vec::with_capacity(im.layers.len() - range.len() + 1);
        let num_old_layers = im.layers.len();
        let mut old_layers = std::mem::replace(&mut im.layers, new_layers).into_iter();
        for _ in 0..range.start {
          im.layers.push(old_layers.next().unwrap());
        }
        im.layers.push(new_layer.clone());
        for _ in range.clone() {
          old_layers.next().unwrap();
        }
        for _ in range.end..num_old_layers {
          im.layers.push(old_layers.next().unwrap());
        }
        assert!(old_layers.next().is_none());

        assert!(range.len() >= 2);
        old_layers_offset += range.len() - 1;
      });

      request_writeback();

      tracing::info!(%image_id, level, ?range, ?blob_ids, "range compaction complete");
    }
  }
}

async fn compact_and_persist_layer(
  image_store: Rc<dyn ImageStore>,
  blobs: Vec<&BlobReader>,
) -> anyhow::Result<Layer> {
  let output_blob_id = generate_blob_id();
  let output_stream = compact_blobs(blobs, "".into())
    .await
    .with_context(|| "failed to initiate blob compaction")?;
  image_store
    .set_blob(&output_blob_id, output_stream)
    .await
    .with_context(|| "failed to write compacted blob")?;
  let reader = BlobReader::open(
    image_store
      .get_blob(&output_blob_id)
      .await
      .with_context(|| "failed to get new blob")?,
  )
  .await
  .with_context(|| "failed to open new blob")?;

  Ok(Layer {
    blob_id: output_blob_id.into(),
    blob: reader,
  })
}

async fn trim_once(
  layers: Vec<Rc<Layer>>,
  image_store: Rc<dyn ImageStore>,
  num_trimmed_bytes: &mut u64,
) -> Vec<Rc<Layer>> {
  let mut page_index_to_layer_index: HashMap<u64, usize> = HashMap::new();

  for (layer_seq, layer) in layers.iter().enumerate() {
    for (page_id, _) in layer.blob.page_index() {
      page_index_to_layer_index.insert(*page_id, layer_seq);
    }
  }

  let mut new_layers: Vec<Rc<Layer>> = Vec::with_capacity(layers.len());

  for (layer_seq, layer) in layers.into_iter().enumerate() {
    // HACK: Only trim once per `trim_once` invocation, for now
    if *num_trimmed_bytes != 0 {
      new_layers.push(layer);
      continue;
    }

    let gen_referenced_pages = || {
      layer
        .blob
        .page_index()
        .iter()
        .filter(|(page_id, _)| page_index_to_layer_index.get(page_id) == Some(&layer_seq))
        .map(|(page_id, page_metadata)| (*page_id, page_metadata.clone()))
    };
    match trim_layer(
      layer.clone(),
      image_store.clone(),
      num_trimmed_bytes,
      gen_referenced_pages,
    )
    .await
    {
      Ok(Some(layer)) => {
        new_layers.push(layer);
      }
      Ok(None) => {}
      Err(e) => {
        tracing::error!(error = ?e, %layer.blob_id, "failed to trim layer");
        new_layers.push(layer);
      }
    }
  }

  new_layers
}

async fn trim_layer<I: Iterator<Item = (u64, PageMetadata)>>(
  layer: Rc<Layer>,
  image_store: Rc<dyn ImageStore>,
  num_trimmed_bytes: &mut u64,
  gen_referenced_pages: impl Fn() -> I,
) -> anyhow::Result<Option<Rc<Layer>>> {
  let total_size = layer.blob.size() - layer.blob.body_offset();

  // Small layer, don't trim
  if total_size <= 1048576 * 16 {
    return Ok(Some(layer));
  }

  let referenced_size = gen_referenced_pages()
    .map(|(_, page)| page.compressed_size)
    .sum::<u64>();

  // drop empty layer
  if referenced_size == 0 {
    *num_trimmed_bytes += total_size;
    return Ok(None);
  }

  // At least 2/3 still referenced, keep it
  if referenced_size >= total_size / 3 * 2 {
    return Ok(Some(layer));
  }

  // Build new blob
  let mut header = BlobHeaderWriter::new(BlobHeaderWriterOptions {
    metadata: layer.blob.metadata().to_owned(),
  });

  for (page_id, page_metadata) in gen_referenced_pages() {
    header.add_page(PageInfoInHeader {
      id: page_id,
      compressed_size: page_metadata.compressed_size,
    })?;
  }
  let header = header.encode();
  let chunks = layer
    .blob
    .backend()
    .clone()
    .stream_chunks(
      layer.blob.body_offset(),
      layer
        .blob
        .page_index()
        .values()
        .map(|x| x.compressed_size)
        .collect(),
    )
    .await?;
  let (tx, rx) = tokio::sync::mpsc::channel::<anyhow::Result<Bytes>>(16);
  let mut tx = Some(tx);
  let tx_work = async {
    // Ensure `tx` is dropped at end of block
    let tx = tx.take().unwrap();

    if tx.send(Ok(header)).await.is_err() {
      return;
    }

    let mut stream_it = chunks.zip(futures::stream::iter(
      layer.blob.page_index().keys().copied(),
    ));

    for (page_id, _) in gen_referenced_pages() {
      let chunk = loop {
        let Some((chunk, chunk_page_id)) = stream_it.next().await else {
          let _ = tx
            .send(Err(anyhow::anyhow!("unexpected end of stream")))
            .await;
          return;
        };
        let chunk = match chunk {
          Ok(x) => x,
          Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
          }
        };
        if chunk_page_id == page_id {
          break chunk;
        }
      };

      if tx.send(Ok(chunk)).await.is_err() {
        return;
      }
    }
  };
  let new_blob_id = generate_blob_id();
  let rx_work = image_store.set_blob(&new_blob_id, ReceiverStream::new(rx).boxed());

  let tx_work = std::pin::pin!(tx_work.map(Ok));

  futures::future::try_join(tx_work, rx_work)
    .await
    .with_context(|| "failed to write new blob")?;

  let new_reader = BlobReader::open(image_store.clone().get_blob(&new_blob_id).await?).await?;

  assert!(total_size > referenced_size);
  *num_trimmed_bytes += total_size - referenced_size;

  Ok(Some(Rc::new(Layer {
    blob_id: new_blob_id.into(),
    blob: new_reader,
  })))
}
