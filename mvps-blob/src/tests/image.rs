use std::rc::Rc;

use bytes::Bytes;
use bytestring::ByteString;
use tempdir::TempDir;

use super::util::create_blob;
use crate::{
  backend::local_fs::LocalFsImageStore,
  blob_reader::{BlobReader, DecompressedPage},
  image::ImageManager,
  interfaces::ImageStore,
  util::generate_image_id,
};

#[tokio::test]
async fn test_create_and_read_image() {
  let dir = TempDir::new("mvps-test").unwrap();

  let image_store = Rc::new(LocalFsImageStore::new(dir.path().to_path_buf()).unwrap());
  let blob1: ByteString = create_blob(
    &*image_store,
    &mut [
      (0, Some(Bytes::from("hello"))),
      (2, Some(Bytes::from("alice"))),
      (5, Some(Bytes::from("some-data"))),
      (10, Some(Bytes::from("other-data"))),
    ],
    None,
  )
  .await
  .unwrap()
  .into();
  let blob2: ByteString = create_blob(
    &*image_store,
    &mut [(2, Some(Bytes::from("bob"))), (5, None)],
    None,
  )
  .await
  .unwrap()
  .into();

  async fn assert_image_ok(image: &ImageManager) {
    assert_eq!(
      image.read_page(0).await.unwrap(),
      Some(DecompressedPage {
        data: Bytes::from_static(b"hello"),
      })
    );
    assert_eq!(image.read_page(1).await.unwrap(), None);
    assert_eq!(
      image.read_page(2).await.unwrap(),
      Some(DecompressedPage {
        data: Bytes::from_static(b"bob"),
      })
    );
    assert_eq!(image.read_page(5).await.unwrap(), None);
    assert_eq!(
      image.read_page(10).await.unwrap(),
      Some(DecompressedPage {
        data: Bytes::from_static(b"other-data"),
      })
    );
  }

  let image_id = ByteString::from(generate_image_id());
  let mut image = ImageManager::open(image_store.clone(), image_id.clone(), &[])
    .await
    .unwrap();
  image
    .push_layer(
      blob1.clone(),
      BlobReader::open(image_store.clone().get_blob(&blob1).await.unwrap(), &[])
        .await
        .unwrap(),
    )
    .unwrap();
  image
    .push_layer(
      blob2.clone(),
      BlobReader::open(image_store.clone().get_blob(&blob2).await.unwrap(), &[])
        .await
        .unwrap(),
    )
    .unwrap();

  assert_image_ok(&image).await;

  image.write_back().await.unwrap();
  drop(image);

  let image = ImageManager::open(image_store, image_id, &[]).await.unwrap();
  assert_image_ok(&image).await;
}
