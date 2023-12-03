use std::rc::Rc;

use bytes::Bytes;
use tempdir::TempDir;

use super::util::create_blob;
use crate::{
  backend::local_fs::{LocalFsImageStore, BLOBS_SUBDIR},
  blob_reader::{BlobReader, DecompressedPage, PagePresence},
  compaction::compact_blobs,
  interfaces::ImageStore,
  util::generate_blob_id,
};

#[tokio::test]
async fn test_create_and_read_blob() {
  let dir = TempDir::new("mvps-test").unwrap();
  println!("dir: {:?}", dir.path());

  let image_store = Rc::new(LocalFsImageStore::new(dir.path().to_path_buf()).unwrap());
  let blob_id = create_blob(
    &*image_store,
    &mut [
      (0, Some(Bytes::from("hello"))),
      (2, Some(Bytes::from("world"))),
      (5, None),
    ],
  )
  .await
  .unwrap();
  println!(
    "blob: {}, file size: {}",
    blob_id,
    std::fs::metadata(dir.path().join(BLOBS_SUBDIR).join(blob_id.as_str()))
      .unwrap()
      .len()
  );

  let reader = BlobReader::open(image_store.get_blob(&blob_id).await.unwrap())
    .await
    .unwrap();
  assert_eq!(
    reader.read_page(0).await.unwrap(),
    PagePresence::Present(DecompressedPage {
      data: Bytes::from_static(b"hello"),
    })
  );
  assert_eq!(reader.read_page(1).await.unwrap(), PagePresence::NotPresent);
  assert_eq!(
    reader.read_page(2).await.unwrap(),
    PagePresence::Present(DecompressedPage {
      data: Bytes::from_static(b"world"),
    })
  );
  assert_eq!(reader.read_page(5).await.unwrap(), PagePresence::Tombstone);
}

#[tokio::test]
async fn test_compact_blobs() {
  let dir = TempDir::new("mvps-test").unwrap();
  println!("dir: {:?}", dir.path());

  let image_store = Rc::new(LocalFsImageStore::new(dir.path().to_path_buf()).unwrap());
  let blob1 = create_blob(
    &*image_store,
    &mut [
      (0, Some(Bytes::from("hello"))),
      (2, Some(Bytes::from("alice"))),
      (5, Some(Bytes::from("some-data"))),
      (10, Some(Bytes::from("other-data"))),
    ],
  )
  .await
  .unwrap();
  let blob2 = create_blob(
    &*image_store,
    &mut [(2, Some(Bytes::from("bob"))), (5, None)],
  )
  .await
  .unwrap();
  let reader1 = BlobReader::open(image_store.clone().get_blob(&blob1).await.unwrap())
    .await
    .unwrap();
  let reader2 = BlobReader::open(image_store.clone().get_blob(&blob2).await.unwrap())
    .await
    .unwrap();

  let output = compact_blobs(vec![reader1, reader2], "".into())
    .await
    .unwrap();
  let output_blob_id = generate_blob_id();
  image_store.set_blob(&output_blob_id, output).await.unwrap();

  let reader = BlobReader::open(image_store.get_blob(&output_blob_id).await.unwrap())
    .await
    .unwrap();
  assert_eq!(
    reader.read_page(0).await.unwrap(),
    PagePresence::Present(DecompressedPage {
      data: Bytes::from_static(b"hello"),
    })
  );
  assert_eq!(reader.read_page(1).await.unwrap(), PagePresence::NotPresent);
  assert_eq!(
    reader.read_page(2).await.unwrap(),
    PagePresence::Present(DecompressedPage {
      data: Bytes::from_static(b"bob"),
    })
  );
  assert_eq!(reader.read_page(5).await.unwrap(), PagePresence::Tombstone);
  assert_eq!(
    reader.read_page(10).await.unwrap(),
    PagePresence::Present(DecompressedPage {
      data: Bytes::from_static(b"other-data"),
    })
  );
}
