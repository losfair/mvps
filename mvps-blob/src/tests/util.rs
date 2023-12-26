use bytes::Bytes;
use futures::{future::ready, StreamExt};
use mvps_proto::blob::{BlobPage, BlobPageCompressionMethod};
use prost::Message;
use rand::Rng;

use crate::{
  blob_crypto::CryptoRootKey,
  blob_writer::{BlobHeaderWriter, BlobHeaderWriterOptions, PageInfoInHeader},
  interfaces::ImageStore,
  util::generate_blob_id,
};

pub async fn create_blob(
  image_store: &dyn ImageStore,
  values: &mut [(u64, Option<Bytes>)],
  root_key: Option<&CryptoRootKey>,
) -> anyhow::Result<String> {
  let id = generate_blob_id();
  let mut header = BlobHeaderWriter::new(
    BlobHeaderWriterOptions {
      metadata: "".into(),
    },
    root_key,
  );

  values.sort_by_key(|(page_id, _)| *page_id);
  let values = &*values;

  let pages = values
    .iter()
    .map(|(page_id, data)| {
      let Some(data) = data else {
        return (*page_id, Bytes::new());
      };
      let compressed = rand::thread_rng().gen::<bool>();
      (
        *page_id,
        Bytes::from(
          BlobPage {
            compression: if compressed {
              BlobPageCompressionMethod::BpcmZstd
            } else {
              BlobPageCompressionMethod::BpcmNone
            }
            .into(),
            data: if compressed {
              zstd::encode_all(&data[..], 0).unwrap().into()
            } else {
              data.clone()
            },
          }
          .encode_to_vec(),
        ),
      )
    })
    .collect::<Vec<_>>();

  for (page_id, page) in &pages {
    header.add_page(PageInfoInHeader {
      id: *page_id,
      compressed_size: page.len() as u64,
    })?;
  }

  let (header, subkey) = header.encode();

  let stream = futures::stream::once(ready(Ok::<_, anyhow::Error>(header)))
    .chain(futures::stream::iter(
      pages
        .iter()
        .map(|(page_id, page)| {
          Ok(Bytes::from(
            subkey.encrypt_with_u64_le_nonce(page.to_vec(), *page_id),
          ))
        })
        .collect::<Vec<_>>(),
    ))
    .boxed();

  image_store.set_blob(&id, stream).await?;

  Ok(id)
}
