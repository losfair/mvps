use std::rc::Rc;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::Either;
use heed::{
  types::{OwnedSlice, OwnedType},
  Database, Env, RoTxn,
};
use mvps_blob::blob_reader::{DecompressedPage, PagePresence};

type BEU64 = heed::types::U64<heed::byteorder::BigEndian>;

#[derive(Clone)]
pub struct BufferStore {
  env: Env,

  // page_id -> (change_count :: LEU64, data)
  // std::u64::MAX -> max_change_count
  // empty data means tombstone
  db: Database<OwnedType<BEU64>, OwnedSlice<u8>>,
}

pub struct BufferStoreSnapshot<'a> {
  txn: heed::RoTxn<'a>,
  db: &'a Database<OwnedType<BEU64>, OwnedSlice<u8>>,
  change_count: u64,
}

pub struct BufferStoreTxn<'a> {
  env: &'a Env,
  txn: Either<(heed::RwTxn<'a, 'a>, u64), heed::RoTxn<'a>>,
  db: &'a Database<OwnedType<BEU64>, OwnedSlice<u8>>,
}

pub struct OwnedBufferStoreTxn {
  bs: Option<Rc<BufferStore>>,
  inner: Option<BufferStoreTxn<'static>>,
}

impl OwnedBufferStoreTxn {
  pub fn get(&self) -> &BufferStoreTxn<'static> {
    self.inner.as_ref().unwrap()
  }

  pub fn get_mut(&mut self) -> &mut BufferStoreTxn<'static> {
    self.inner.as_mut().unwrap()
  }

  pub async fn commit(mut self) -> anyhow::Result<()> {
    self.inner.take().unwrap().commit().await
  }
}

impl Drop for OwnedBufferStoreTxn {
  fn drop(&mut self) {
    self.inner.take();
    self.bs.take();
  }
}

impl BufferStore {
  pub fn new(env: Env, image_id: ByteString) -> anyhow::Result<Self> {
    let db = env
      .create_database(Some(&*image_id))
      .map_err(|e| anyhow::anyhow!("create database failed: {:?}", e))?;

    Ok(Self { env, db })
  }

  pub fn advance_change_count(&self, target: u64) -> anyhow::Result<u64> {
    let mut txn = self
      .env
      .write_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    let curr_change_count = read_change_count(&self.db, &txn)?;
    if curr_change_count >= target {
      return Ok(curr_change_count);
    }

    self
      .db
      .put(&mut txn, &BEU64::new(std::u64::MAX), &target.to_le_bytes())
      .map_err(|e| anyhow::anyhow!("write max change count failed: {:?}", e))?;
    txn
      .commit()
      .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;
    Ok(curr_change_count)
  }

  pub fn begin_txn_owned(self: Rc<Self>) -> anyhow::Result<OwnedBufferStoreTxn> {
    let txn = BufferStoreTxn {
      env: &self.env,
      txn: Either::Right(
        self
          .env
          .read_txn()
          .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?,
      ),
      db: &self.db,
    };
    let txn = unsafe { std::mem::transmute::<BufferStoreTxn, BufferStoreTxn<'static>>(txn) };

    Ok(OwnedBufferStoreTxn {
      bs: Some(self),
      inner: Some(txn),
    })
  }

  pub fn snapshot_for_checkpoint(&self) -> anyhow::Result<BufferStoreSnapshot> {
    let txn = self
      .env
      .read_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    let change_count = read_change_count(&self.db, &txn)?;

    Ok(BufferStoreSnapshot {
      txn,
      db: &self.db,
      change_count,
    })
  }

  pub fn unbuffer_pages(&self, pages: Vec<(u64, u64)>) -> anyhow::Result<()> {
    let mut txn = self
      .env
      .write_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    for (page_id, expected_change_count) in &pages {
      let data = match self
        .db
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

      self
        .db
        .delete(&mut txn, &BEU64::new(*page_id))
        .map_err(|e| anyhow::anyhow!("delete page failed: {:?}", e))?;
    }
    txn
      .commit()
      .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;
    Ok(())
  }

  pub fn len(&self) -> anyhow::Result<u64> {
    let txn = self
      .env
      .read_txn()
      .map_err(|e| anyhow::anyhow!("begin txn failed: {:?}", e))?;
    let len = self
      .db
      .len(&txn)
      .map_err(|e| anyhow::anyhow!("get len failed: {:?}", e))?;
    Ok(len)
  }
}

impl<'a> BufferStoreTxn<'a> {
  pub async fn read_page(&self, page_id: u64) -> anyhow::Result<PagePresence> {
    if page_id == std::u64::MAX {
      anyhow::bail!("cannot read page -1");
    }

    let txn: &heed::RoTxn = match &self.txn {
      Either::Left(x) => &x.0,
      Either::Right(x) => x,
    };

    let data = match self
      .db
      .get(txn, &BEU64::new(page_id))
      .map_err(|e| anyhow::anyhow!("read page failed: {:?}", e))?
    {
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
      Either::Right(ref txn) => read_change_count(&self.db, txn)?,
    };

    let write_txn = self
      .env
      .write_txn()
      .map_err(|e| anyhow::anyhow!("begin write txn failed: {:?}", e))?;
    let curr_change_count = read_change_count(&self.db, &write_txn)?;
    if curr_change_count != prev_change_count {
      return Ok(false);
    }

    self.txn = Either::Left((write_txn, curr_change_count));
    Ok(true)
  }

  pub async fn write_page(&mut self, page_id: u64, data: Option<Bytes>) -> anyhow::Result<()> {
    if page_id == std::u64::MAX {
      anyhow::bail!("cannot write page -1");
    }

    if !self.lock_for_write().await? {
      anyhow::bail!("transaction cannot be locked");
    }

    let (txn, change_count) = match &mut self.txn {
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

    self
      .db
      .put(txn, &BEU64::new(page_id), &buf)
      .map_err(|e| anyhow::anyhow!("write page failed: {:?}", e))?;

    Ok(())
  }

  pub async fn commit(self) -> anyhow::Result<()> {
    match self.txn {
      Either::Left((mut txn, change_count)) => {
        self
          .db
          .put(
            &mut txn,
            &BEU64::new(std::u64::MAX),
            &(change_count + 1).to_le_bytes(),
          )
          .map_err(|e| anyhow::anyhow!("write max change count failed: {:?}", e))?;
        txn
          .commit()
          .map_err(|e| anyhow::anyhow!("commit failed: {:?}", e))?;
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
        if page_id == std::u64::MAX {
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

fn read_change_count(
  db: &Database<OwnedType<BEU64>, OwnedSlice<u8>>,
  txn: &RoTxn,
) -> anyhow::Result<u64> {
  match db
    .get(txn, &BEU64::new(std::u64::MAX))
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
