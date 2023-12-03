use std::{rc::Rc, time::Duration};

use bytes::Bytes;
use bytestring::ByteString;
use heed::EnvOpenOptions;
use mvps_blob::{
  backend::local_fs::LocalFsImageStore, image::ImageManager, interfaces::ImageStore,
  util::generate_image_id,
};
use rand::RngCore;
use tempdir::TempDir;
use tracing_test::traced_test;

use crate::{
  config::{CompactionThreshold, JitteredInterval, LayeredStoreConfig},
  layered_store::LayeredStore,
};

#[tokio::test]
#[traced_test]
async fn test_layered_store() {
  let local = tokio::task::LocalSet::new();
  local
    .run_until(async move {
      let dir = TempDir::new("mvps-test").unwrap();
      let image_store = Rc::new(LocalFsImageStore::new(dir.path().to_path_buf()).unwrap());
      let image_id = ByteString::from(generate_image_id());
      std::fs::create_dir_all(dir.path().join("buffer")).unwrap();
      let env = EnvOpenOptions::new()
        .max_dbs(16384)
        .open(dir.path().join("buffer"))
        .unwrap();

      let open_layered_store = || async {
        LayeredStore::new(
          image_store.clone(),
          image_id.clone(),
          Some(env.clone()),
          LayeredStoreConfig {
            compaction_thresholds: vec![],
            compaction_interval: JitteredInterval {
              min_ms: 1000,
              jitter_ms: 10,
            },
            disable_image_store_write: false,
          },
        )
        .await
        .unwrap()
      };
      let mut layered_store = open_layered_store().await;

      // write, abort, read, write, commit, read
      {
        let mut txn = layered_store.begin_txn().unwrap();
        assert_eq!(txn.read_page(0).await.unwrap(), None);
        txn
          .write_page(0, Some(Bytes::from_static(b"hello")))
          .await
          .unwrap();
        assert_eq!(
          txn.read_page(0).await.unwrap(),
          Some(Bytes::from_static(b"hello"))
        );

        drop(txn);

        let txn = layered_store.begin_txn().unwrap();
        assert_eq!(txn.read_page(0).await.unwrap(), None);
        drop(txn);

        let mut txn = layered_store.begin_txn().unwrap();
        assert_eq!(txn.read_page(0).await.unwrap(), None);
        txn
          .write_page(0, Some(Bytes::from_static(b"hello")))
          .await
          .unwrap();
        assert_eq!(
          txn.read_page(0).await.unwrap(),
          Some(Bytes::from_static(b"hello"))
        );

        txn.commit().await.unwrap();

        let txn = layered_store.begin_txn().unwrap();
        assert_eq!(
          txn.read_page(0).await.unwrap(),
          Some(Bytes::from_static(b"hello"))
        );
        assert_eq!(txn.read_page(1).await.unwrap(), None);
      }

      // checkpoint
      layered_store.request_checkpoint(true).await.unwrap();
      layered_store.shutdown().await.unwrap();

      // ensure data is checkpointed
      {
        let im = ImageManager::open(image_store.clone(), image_id.clone())
          .await
          .unwrap();
        assert_eq!(
          im.read_page(0).await.unwrap().map(|x| x.data),
          Some(Bytes::from_static(b"hello"))
        );
      }

      // read from checkpointed data
      layered_store = open_layered_store().await;

      {
        let txn = layered_store.begin_txn().unwrap();
        assert_eq!(
          txn.read_page(0).await.unwrap(),
          Some(Bytes::from_static(b"hello"))
        );
        assert_eq!(txn.read_page(1).await.unwrap(), None);
      }

      layered_store.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[traced_test]
async fn test_compaction() {
  let local = tokio::task::LocalSet::new();
  local
    .run_until(async move {
      let dir = TempDir::new("mvps-test").unwrap();
      let image_store = Rc::new(LocalFsImageStore::new(dir.path().to_path_buf()).unwrap());
      let image_id = ByteString::from(generate_image_id());
      std::fs::create_dir_all(dir.path().join("buffer")).unwrap();
      let env = EnvOpenOptions::new()
        .max_dbs(16384)
        .open(dir.path().join("buffer"))
        .unwrap();
      let open_layered_store = |image_id: ByteString, num_compaction_levels: usize| {
        let image_store = &image_store;
        let env = &env;
        async move {
          LayeredStore::new(
            image_store.clone(),
            image_id.clone(),
            Some(env.clone()),
            LayeredStoreConfig {
              compaction_thresholds: [
                CompactionThreshold {
                  max_input_size: 9 * 1024,
                  min_output_size: 12 * 1024,
                  max_output_size: 20 * 1024,
                  pressure_level: 0,
                },
                CompactionThreshold {
                  max_input_size: 21 * 1024,
                  min_output_size: 30 * 1024,
                  max_output_size: 50 * 1024,
                  pressure_level: 0,
                },
              ][..num_compaction_levels]
                .to_vec(),
              compaction_interval: JitteredInterval {
                min_ms: 100,
                jitter_ms: 10,
              },
              disable_image_store_write: false,
            },
          )
          .await
          .unwrap()
        }
      };

      let mut pages = vec![
        vec![0u8; 4096],
        vec![0u8; 4096],
        vec![0u8; 16384],
        vec![0u8; 4096],
        vec![0u8; 4096],
        vec![0u8; 4096],
      ];

      for p in &mut pages {
        rand::thread_rng().fill_bytes(&mut p[..]);
      }

      for (page_id, page) in pages.iter().enumerate() {
        let layered_store = open_layered_store(image_id.clone(), 0).await;
        let mut txn = layered_store.begin_txn().unwrap();

        // Flip all bits in previous page & write the current page
        if page_id != 0 {
          let mut prev_page = txn
            .read_page(page_id as u64 - 1)
            .await
            .unwrap()
            .unwrap()
            .to_vec();
          for b in &mut prev_page {
            *b ^= 0xff;
          }
          txn
            .write_page(page_id as u64 - 1, Some(prev_page.into()))
            .await
            .unwrap();
        }

        txn
          .write_page(page_id as u64, Some(Bytes::copy_from_slice(&page)))
          .await
          .unwrap();
        txn.commit().await.unwrap();
        layered_store.request_checkpoint(true).await.unwrap();
        layered_store.shutdown().await.unwrap();
      }

      let num_pages = pages.len();

      for page in pages.iter_mut().take(num_pages - 1) {
        for b in page {
          *b ^= 0xff;
        }
      }

      // Copy original image
      let image2_id = ByteString::from(generate_image_id());

      {
        let mut image2_info = image_store.get_image_info(&image_id).await.unwrap();
        assert_eq!(image2_info.layers.len(), 6);
        image2_info.change_count = 0;

        image_store
          .set_image_info(&image2_id, &image2_info)
          .await
          .unwrap();
      }

      // Compact image1 with 1 level
      {
        let layered_store = open_layered_store(image_id.clone(), 1).await;
        while layered_store.test_get_base().layers.len() != 4 {
          tokio::time::sleep(Duration::from_millis(50)).await;
        }
        layered_store.shutdown().await.unwrap();

        let image_info = image_store.get_image_info(&image_id).await.unwrap();
        println!("image: {:?}", image_info);
        let image = ImageManager::open(image_store.clone(), image_id.clone())
          .await
          .unwrap();
        assert_eq!(image.layers.len(), 4);

        for (page_id, page) in pages.iter().enumerate() {
          assert_eq!(
            image.read_page(page_id as u64).await.unwrap().unwrap().data[..],
            page[..]
          );
        }
      }

      // Compact image2 with 2 levels
      {
        let layered_store = open_layered_store(image2_id.clone(), 2).await;
        while layered_store.test_get_base().layers.len() != 2 {
          tokio::time::sleep(Duration::from_millis(50)).await;
        }
        layered_store.shutdown().await.unwrap();

        let image_info = image_store.get_image_info(&image2_id).await.unwrap();
        println!("image: {:?}", image_info);
        let image = ImageManager::open(image_store.clone(), image2_id.clone())
          .await
          .unwrap();
        assert_eq!(image.layers.len(), 2);
        for (page_id, page) in pages.iter().enumerate() {
          assert_eq!(
            image.read_page(page_id as u64).await.unwrap().unwrap().data[..],
            page[..]
          );
        }
      }
    })
    .await;
}
