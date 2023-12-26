mod async_txn;

use std::sync::Arc;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::Either;
use heed::{
  types::{OwnedSlice, OwnedType},
  Database, Env,
};
use mvps_blob::blob_reader::{DecompressedPage, PagePresence};
use tokio::{
  sync::{Mutex, OwnedMutexGuard},
  task::spawn_blocking,
};

use self::async_txn::{AsyncRoTxn, AsyncRwTxn};

type BEU64 = heed::types::U64<heed::byteorder::BigEndian>;

#[repr(u64)]
#[derive(Copy, Clone, Debug)]
enum SpecialKey {
  MaxChangeCount = std::u64::MAX,
  WriterId = std::u64::MAX - 1,
}

pub struct BufferStore {
  env: Env,

  // page_id -> (change_count :: LEU64, data)
  // (x >> 32) == std::u32::MAX -> internal
  // empty data means tombstone
  db: Database<OwnedType<BEU64>, OwnedSlice<u8>>,

  // Process-local write lock
  local_write_lock: Arc<Mutex<()>>,
}

pub struct BufferStoreSnapshot<'a> {
  txn: heed::RoTxn<'a>,
  db: &'a Database<OwnedType<BEU64>, OwnedSlice<u8>>,
  change_count: u64,
}

pub struct BufferStoreTxn {
  env: Env,
  txn: Either<((AsyncRwTxn, OwnedMutexGuard<()>), u64), AsyncRoTxn>,
  db: Database<OwnedType<BEU64>, OwnedSlice<u8>>,
  local_write_lock: Arc<Mutex<()>>,
}

impl BufferStore {
  pub fn new(env: Env, image_id: ByteString) -> anyhow::Result<Self> {
    let db = env
      .create_database(Some(&*image_id))
      .map_err(|e| anyhow::anyhow!("create database failed: {:?}", e))?;

    Ok(Self {
      env,
      db,
      local_write_lock: Arc::new(Mutex::new(())),
    })
  }

  pub fn get_writer_id(&self) -> anyhow::Result<Option<ByteString>> {
    let txn = self
      .env
      .read_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    let data = match self
      .db
      .get(&txn, &BEU64::new(SpecialKey::WriterId as u64))
      .map_err(|e| anyhow::anyhow!("read writer id failed: {:?}", e))?
    {
      Some(x) => x,
      None => return Ok(None),
    };

    Ok(Some(ByteString::from(String::from_utf8(data)?)))
  }

  pub fn set_writer_id_if_not_exists(&self, writer_id: &ByteString) -> anyhow::Result<()> {
    let _guard = self.local_write_lock.try_lock()?;
    let mut txn = self
      .env
      .write_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    match self
      .db
      .get(&txn, &BEU64::new(SpecialKey::WriterId as u64))
      .map_err(|e| anyhow::anyhow!("read writer id failed: {:?}", e))?
    {
      Some(_) => {
        anyhow::bail!("writer id already exists")
      }
      None => {
        self
          .db
          .put(
            &mut txn,
            &BEU64::new(SpecialKey::WriterId as u64),
            writer_id.as_bytes(),
          )
          .map_err(|e| anyhow::anyhow!("write writer id failed: {:?}", e))?;
        txn
          .commit()
          .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;
        return Ok(());
      }
    }
  }

  pub async fn advance_change_count(&self, target: u64) -> anyhow::Result<u64> {
    let _guard = self.local_write_lock.try_lock()?;
    let txn = AsyncRwTxn::begin(self.env.clone()).await?;
    let curr_change_count = read_change_count(&self.db, &txn).await?;
    if curr_change_count >= target {
      return Ok(curr_change_count);
    }
    if curr_change_count != 0 {
      anyhow::bail!("local change count is non-zero but lags behind remote, corruption? local_change_count={}, target={}", curr_change_count, target);
    }

    let db = self.db.clone();
    txn
      .with(move |txn| {
        db.put(
          txn,
          &BEU64::new(SpecialKey::MaxChangeCount as u64),
          &target.to_le_bytes(),
        )
        .map_err(|e| anyhow::anyhow!("write max change count failed: {:?}", e))
      })
      .await?;

    txn
      .commit()
      .await
      .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;
    Ok(curr_change_count)
  }

  pub async fn begin_txn(&self) -> anyhow::Result<BufferStoreTxn> {
    let txn = AsyncRoTxn::begin(self.env.clone()).await?;
    let txn = BufferStoreTxn {
      env: self.env.clone(),
      txn: Either::Right(txn),
      db: self.db.clone(),
      local_write_lock: self.local_write_lock.clone(),
    };

    Ok(txn)
  }

  pub fn snapshot_for_checkpoint(&self) -> anyhow::Result<BufferStoreSnapshot> {
    let txn = self
      .env
      .read_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    let change_count = read_change_count_sync(&self.db, &txn)?;

    Ok(BufferStoreSnapshot {
      txn,
      db: &self.db,
      change_count,
    })
  }

  pub async fn unbuffer_pages(&self, pages: Vec<(u64, u64)>) -> anyhow::Result<()> {
    let _guard = self.local_write_lock.lock().await;
    let env = self.env.clone();
    let db = self.db.clone();

    tokio::task::spawn_blocking(move || {
      let mut txn = env
        .write_txn()
        .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
      for (page_id, expected_change_count) in &pages {
        let data = match db
          .get(&txn, &BEU64::new(*page_id))
          .map_err(|e| anyhow::anyhow!("read page failed: {:?}", e))?
        {
          Some(x) => x,
          None => continue,
        };

        if data.len() < 8 {
          anyhow::bail!("invalid data length");
        }

        let actual_change_count = u64::from_le_bytes(data[..8].try_into().unwrap());
        if *expected_change_count != actual_change_count {
          continue;
        }

        db.delete(&mut txn, &BEU64::new(*page_id))
          .map_err(|e| anyhow::anyhow!("delete page failed: {:?}", e))?;
      }
      txn
        .commit()
        .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;
      Ok(())
    })
    .await??;

    Ok(())
  }

  pub async fn len(&self) -> anyhow::Result<u64> {
    let db = self.db.clone();
    AsyncRoTxn::begin(self.env.clone())
      .await?
      .with(move |txn| {
        db.len(txn)
          .map_err(|e| anyhow::anyhow!("get len failed: {:?}", e))
      })
      .await
  }

  pub async fn fsync(&self) -> anyhow::Result<()> {
    let env = self.env.clone();
    spawn_blocking(move || {
      env
        .force_sync()
        .map_err(|e| anyhow::anyhow!("sync failed: {:?}", e))
    })
    .await??;
    Ok(())
  }
}

impl BufferStoreTxn {
  pub async fn read_page(&self, page_id: u64) -> anyhow::Result<PagePresence> {
    if page_id >> 32 == std::u32::MAX as u64 {
      anyhow::bail!("cannot read internal pages");
    }

    let data = match &self.txn {
      Either::Left(x) => {
        let db = self.db.clone();
        x.0
           .0
          .with(move |x| {
            db.get(x, &BEU64::new(page_id))
              .map_err(|x| format!("{:?}", x))
          })
          .await
      }
      Either::Right(x) => {
        let db = self.db.clone();
        x.with(move |x| {
          db.get(x, &BEU64::new(page_id))
            .map_err(|x| format!("{:?}", x))
        })
        .await
      }
    }
    .map_err(|x| anyhow::anyhow!("read page failed: {}", x))?;

    let data = match data {
      Some(x) => x,
      None => return Ok(PagePresence::NotPresent),
    };

    if data.len() < 8 {
      anyhow::bail!("invalid data length");
    }

    if data.len() == 8 {
      return Ok(PagePresence::Tombstone);
    }

    let data = Bytes::from(data).slice(8..);

    Ok(PagePresence::Present(DecompressedPage { data }))
  }

  pub async fn lock_for_write(&mut self) -> anyhow::Result<bool> {
    let prev_change_count = match self.txn {
      Either::Left(_) => return Ok(true),
      Either::Right(ref txn) => read_change_count(&self.db, txn).await?,
    };

    let local_guard = self.local_write_lock.clone().lock_owned().await;

    // This can block on futex if we don't acquire the local lock
    let write_txn = AsyncRwTxn::begin(self.env.clone()).await?;
    let curr_change_count = read_change_count(&self.db, &write_txn).await?;
    if curr_change_count != prev_change_count {
      return Ok(false);
    }

    self.txn = Either::Left(((write_txn, local_guard), curr_change_count));
    Ok(true)
  }

  pub async fn write_page(&mut self, page_id: u64, data: Option<Bytes>) -> anyhow::Result<()> {
    if page_id >> 32 == std::u32::MAX as u64 {
      anyhow::bail!("cannot write internal pages");
    }

    if !self.lock_for_write().await? {
      anyhow::bail!("transaction cannot be locked");
    }

    let ((txn, _), change_count) = match &mut self.txn {
      Either::Left(x) => x,
      Either::Right(_) => unreachable!(),
    };

    if let Some(data) = &data {
      if data.is_empty() {
        anyhow::bail!("cannot write empty page");
      }
    }

    let data = data.unwrap_or_default();

    let mut buf = Vec::with_capacity(8 + data.len());
    buf.extend_from_slice(&change_count.to_le_bytes());
    buf.extend_from_slice(&data);

    let db = self.db.clone();

    txn
      .with(move |txn| {
        db.put(txn, &BEU64::new(page_id), &buf)
          .map_err(|e| anyhow::anyhow!("write page failed: {:?}", e))
      })
      .await?;

    Ok(())
  }

  pub async fn commit(self) -> anyhow::Result<()> {
    match self.txn {
      Either::Left(((txn, _guard), change_count)) => {
        let db = self.db.clone();
        txn
          .with(move |txn| {
            db.put(
              txn,
              &BEU64::new(SpecialKey::MaxChangeCount as u64),
              &(change_count + 1).to_le_bytes(),
            )
            .map_err(|e| anyhow::anyhow!("write max change count failed: {:?}", e))
          })
          .await?;
        txn
          .commit()
          .await
          .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;

        // _guard is dropped here
        Ok(())
      }
      Either::Right(_) => Ok(()),
    }
  }
}

pub struct BufferStoreSnapshotIterator<'a> {
  it: heed::RoIter<'a, OwnedType<BEU64>, OwnedSlice<u8>>,
}

impl<'a> BufferStoreSnapshot<'a> {
  pub fn iter(&self) -> anyhow::Result<BufferStoreSnapshotIterator> {
    Ok(BufferStoreSnapshotIterator {
      it: self
        .db
        .iter(&self.txn)
        .map_err(|e| anyhow::anyhow!("begin iter failed: {:?}", e))?,
    })
  }

  pub fn change_count(&self) -> u64 {
    self.change_count
  }
}

impl<'a> Iterator for BufferStoreSnapshotIterator<'a> {
  type Item = anyhow::Result<(u64, u64, Option<Bytes>)>;

  fn next(&mut self) -> Option<Self::Item> {
    match self.it.next() {
      None => None,
      Some(Err(e)) => Some(Err(anyhow::anyhow!("iter failed: {:?}", e))),
      Some(Ok((k, v))) => {
        if v.len() < 8 {
          return Some(Err(anyhow::anyhow!("invalid data length")));
        }
        let page_id = k.get();
        if page_id >> 32 == std::u32::MAX as u64 {
          return None;
        }

        let change_count = u64::from_le_bytes(v[..8].try_into().unwrap());

        let data = if v.len() == 8 {
          None
        } else {
          Some(Bytes::from(v).slice(8..))
        };
        Some(Ok((page_id, change_count, data)))
      }
    }
  }
}

async fn read_change_count(
  db: &Database<OwnedType<BEU64>, OwnedSlice<u8>>,
  txn: &AsyncRoTxn,
) -> anyhow::Result<u64> {
  let db = db.clone();
  txn.with(move |txn| read_change_count_sync(&db, txn)).await
}

fn read_change_count_sync(
  db: &Database<OwnedType<BEU64>, OwnedSlice<u8>>,
  txn: &heed::RoTxn<'_, ()>,
) -> anyhow::Result<u64> {
  match db
    .get(txn, &BEU64::new(SpecialKey::MaxChangeCount as u64))
    .map_err(|e| anyhow::anyhow!("read max change count failed: {:?}", e))?
  {
    Some(x) => {
      if x.len() != 8 {
        anyhow::bail!("invalid max change count length");
      }
      Ok(u64::from_le_bytes(x.try_into().unwrap()))
    }
    None => Ok(0),
  }
}
