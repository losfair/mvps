use std::{
  collections::HashMap,
  fs::{self, File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  os::unix::fs::FileExt,
  path::{Path, PathBuf},
  sync::{Arc, atomic::{AtomicBool, Ordering}},
};

use bytes::Bytes;
use bytestring::ByteString;
use mvps_blob::blob_reader::{DecompressedPage, PagePresence};
use parking_lot::RwLock;
use tokio::{
  sync::{Mutex, OwnedMutexGuard},
  task::spawn_blocking,
};

// -- WAL file format constants --
//
// On-disk layout (one file per image):
//
//   [WAL Header — 32 bytes]
//   [Record 0]
//   [Record 1]
//   ...
//   [Record N]
//
// Header:
//   magic       : [u8; 8]  = b"MVPSWAL\0"
//   version     : u32 LE   = 1
//   flags       : u32 LE   = 0  (reserved)
//   reserved    : [u8; 16]
//
// Record:
//   record_type : u8
//   padding     : [u8; 3]  = 0
//   seq_no      : u64 LE         monotonically increasing from 0 per WAL file
//   payload_len : u32 LE
//   payload     : [u8; payload_len]
//   crc32c      : u32 LE         over (record_type || padding || seq_no || payload_len || payload)
//
// The sequence number is a defence-in-depth mechanism on top of CRC32C: during
// recovery, a record whose seq_no does not equal the expected value is treated
// as corruption even if its CRC passes, reducing the probability of silently
// accepting a reordered or stale write from ~2^-32 to ~2^-96.

const WAL_MAGIC: &[u8; 8] = b"MVPSWAL\0";
const WAL_VERSION: u32 = 1;
const WAL_HEADER_SIZE: u64 = 32;

const RECORD_TYPE_PAGE_WRITE: u8 = 0x01;
const RECORD_TYPE_PAGE_TOMBSTONE: u8 = 0x02;
const RECORD_TYPE_SET_MAX_CHANGE_COUNT: u8 = 0x03;
const RECORD_TYPE_SET_WRITER_ID: u8 = 0x04;
const RECORD_TYPE_DELETE_PAGE: u8 = 0x05;
const RECORD_TYPE_COMMIT_BARRIER: u8 = 0x06;

const RECORD_HEADER_SIZE: usize = 16; // record_type(1) + padding(3) + seq_no(8) + payload_len(4)
const RECORD_TRAILER_SIZE: usize = 4; // crc32c(4)

// Within a PageWrite payload: page_id(8) + change_count(8) + data
const PAGE_WRITE_PAYLOAD_PREFIX: usize = 16;

// -- In-memory index (no page data) --

#[derive(Clone)]
struct PageIndexEntry {
  change_count: u64,
  location: PageLocation,
}

#[derive(Clone)]
enum PageLocation {
  /// Page data lives in the WAL file at the given offset and length.
  Present { file_offset: u64, data_len: u32 },
  /// Tombstone — page was explicitly deleted.
  Tombstone,
}

/// In-memory index over the WAL file.
///
/// # Invariant
///
/// After recovery (and at all times during normal operation), every
/// `PageLocation::Present { file_offset, data_len }` in `pages` refers to a
/// byte range `[file_offset, file_offset + data_len)` that falls within a
/// committed record in the WAL file pointed to by `reader`. Reads via
/// `read_page_data` against `reader` at those coordinates will return exactly
/// the page data that was written.
///
/// `max_change_count` equals the value stored in the last committed
/// `SetMaxChangeCount` record. `writer_id` equals the value stored in the
/// last committed `SetWriterId` record (or `None` if none was written).
struct WalIndex {
  pages: HashMap<u64, PageIndexEntry>,
  max_change_count: u64,
  writer_id: Option<ByteString>,
  /// Read handle for pread-based lookups (`FileExt::read_exact_at`).
  /// Swapped atomically (under write-lock) during compaction.
  reader: Arc<File>,
}

// -- WAL file I/O --

/// Append-only writer for a WAL file.
///
/// # Invariant
///
/// `position` always equals the current seek position of `file`, which is
/// also the byte offset at which the next `append_record` will begin writing.
/// `next_seq` is the sequence number that will be embedded in the next record.
struct WalWriter {
  file: File,
  next_seq: u64,
  position: u64,
}

impl WalWriter {
  /// Open an existing WAL (replaying and truncating to the last valid commit
  /// barrier) or create a fresh one. The WAL file is exclusively `flock`-ed
  /// to prevent concurrent access from another process.
  ///
  /// # Correctness — recovery
  ///
  /// `replay` scans records sequentially. Records are buffered in a pending
  /// set and only applied to the index when a valid `CommitBarrier` record is
  /// reached.  If the scan encounters a CRC mismatch, a sequence-number gap,
  /// or a truncated read, it stops immediately: the file is then truncated to
  /// the byte offset of the last valid `CommitBarrier`. This guarantees that
  /// only fully committed transactions are recovered.
  ///
  /// Stale `.wal.new` files (from an interrupted compaction) are removed
  /// before replay begins — they are never authoritative.
  fn open_or_create(path: &Path) -> anyhow::Result<(Self, WalIndex)> {
    // Cleanup interrupted compaction
    let new_path = wal_new_path(path);
    if new_path.exists() {
      fs::remove_file(&new_path)?;
    }

    if path.exists() {
      let (index_no_reader, next_seq, valid_end) = Self::replay(path)?;
      let file = OpenOptions::new().read(true).write(true).open(path)?;
      lock_exclusive(&file, path)?;
      file.set_len(valid_end)?;

      let position = valid_end;
      let mut writer = WalWriter { file, next_seq, position };
      writer.file.seek(SeekFrom::Start(position))?;

      let reader = Arc::new(open_reader(path)?);
      let index = WalIndex {
        pages: index_no_reader.pages,
        max_change_count: index_no_reader.max_change_count,
        writer_id: index_no_reader.writer_id,
        reader,
      };
      Ok((writer, index))
    } else {
      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
      lock_exclusive(&file, path)?;
      Self::write_header(&mut file)?;
      file.sync_all()?;

      let reader = Arc::new(open_reader(path)?);
      let index = WalIndex {
        pages: HashMap::new(),
        max_change_count: 0,
        writer_id: None,
        reader,
      };
      Ok((
        WalWriter { file, next_seq: 0, position: WAL_HEADER_SIZE },
        index,
      ))
    }
  }

  fn write_header(file: &mut File) -> anyhow::Result<()> {
    let mut header = [0u8; WAL_HEADER_SIZE as usize];
    header[..8].copy_from_slice(WAL_MAGIC);
    header[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
    file.write_all(&header)?;
    Ok(())
  }

  /// Replay a WAL file, building an index with file offsets (no page data in memory).
  /// Returns (index_without_reader, next_seq, valid_end_offset).
  ///
  /// # Correctness — two-phase commit replay
  ///
  /// Records between two `CommitBarrier`s form an atomic unit. The replay
  /// loop buffers non-barrier records into `pending`. Only when a valid
  /// `CommitBarrier` is encountered are the pending records applied to
  /// `committed`. If the file ends mid-transaction (no final barrier), the
  /// pending records are silently discarded and `last_commit_end` points to
  /// the byte after the last valid barrier — the caller truncates to this.
  ///
  /// # Correctness — `committed_next_seq`
  ///
  /// The returned `next_seq` must be the sequence number that immediately
  /// follows the last *committed* `CommitBarrier`, **not** the total number
  /// of records scanned. The file is truncated to `last_commit_end`, which
  /// removes any trailing uncommitted records. If `next_seq` included those
  /// records, new writes would use inflated sequence numbers, creating a gap
  /// that causes the next recovery to reject valid committed data.
  fn replay(path: &Path) -> anyhow::Result<(WalIndexNoReader, u64, u64)> {
    let mut file = File::open(path)?;

    let mut header = [0u8; WAL_HEADER_SIZE as usize];
    if file.read_exact(&mut header).is_err() {
      anyhow::bail!("WAL file too short for header");
    }
    if &header[..8] != WAL_MAGIC {
      anyhow::bail!("invalid WAL magic");
    }
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if version != WAL_VERSION {
      anyhow::bail!("unsupported WAL version: {}", version);
    }

    let mut committed = WalIndexNoReader::default();
    let mut pending: Vec<PendingIndexRecord> = Vec::new();
    let mut expected_seq: u64 = 0;
    let mut last_commit_end: u64 = WAL_HEADER_SIZE;
    let mut committed_next_seq: u64 = 0;
    let mut current_pos: u64 = WAL_HEADER_SIZE;

    loop {
      let mut rec_header = [0u8; RECORD_HEADER_SIZE];
      match file.read_exact(&mut rec_header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
        Err(e) => return Err(e.into()),
      }

      let record_type = rec_header[0];
      let seq_no = u64::from_le_bytes(rec_header[4..12].try_into().unwrap());
      let payload_len = u32::from_le_bytes(rec_header[12..16].try_into().unwrap()) as usize;

      if seq_no != expected_seq {
        tracing::warn!(expected_seq, actual_seq = seq_no, "WAL sequence mismatch, truncating");
        break;
      }

      let mut payload = vec![0u8; payload_len];
      match file.read_exact(&mut payload) {
        Ok(()) => {}
        Err(_) => break,
      }

      let mut crc_bytes = [0u8; 4];
      match file.read_exact(&mut crc_bytes) {
        Ok(()) => {}
        Err(_) => break,
      }
      let stored_crc = u32::from_le_bytes(crc_bytes);
      let computed_crc = compute_crc(&rec_header, &payload);

      if stored_crc != computed_crc {
        tracing::warn!(current_pos, "WAL CRC mismatch, truncating");
        break;
      }

      let data_file_offset = current_pos + RECORD_HEADER_SIZE as u64 + PAGE_WRITE_PAYLOAD_PREFIX as u64;

      let record_size = RECORD_HEADER_SIZE as u64 + payload_len as u64 + RECORD_TRAILER_SIZE as u64;
      current_pos += record_size;
      expected_seq += 1;

      if record_type == RECORD_TYPE_COMMIT_BARRIER {
        for rec in pending.drain(..) {
          apply_pending_record(&mut committed, rec);
        }
        last_commit_end = current_pos;
        committed_next_seq = expected_seq;
      } else {
        match parse_index_record(record_type, &payload, data_file_offset) {
          Ok(rec) => pending.push(rec),
          Err(e) => {
            tracing::warn!(error = ?e, "WAL record parse error, truncating");
            break;
          }
        }
      }
    }

    Ok((committed, committed_next_seq, last_commit_end))
  }

  fn append_record(&mut self, record_type: u8, payload: &[u8]) -> anyhow::Result<()> {
    let mut rec_header = [0u8; RECORD_HEADER_SIZE];
    rec_header[0] = record_type;
    rec_header[4..12].copy_from_slice(&self.next_seq.to_le_bytes());
    rec_header[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());

    let crc = compute_crc(&rec_header, payload);

    self.file.write_all(&rec_header)?;
    self.file.write_all(payload)?;
    self.file.write_all(&crc.to_le_bytes())?;

    self.position += RECORD_HEADER_SIZE as u64 + payload.len() as u64 + RECORD_TRAILER_SIZE as u64;
    self.next_seq += 1;
    Ok(())
  }

  /// Append a PageWrite record and return the on-disk location of the page
  /// data (offset and length) so the caller can store it in the index.
  fn append_page_write(
    &mut self,
    page_id: u64,
    change_count: u64,
    data: &[u8],
  ) -> anyhow::Result<PageLocation> {
    let data_offset = self.position + RECORD_HEADER_SIZE as u64 + PAGE_WRITE_PAYLOAD_PREFIX as u64;
    let data_len = data.len() as u32;

    let mut payload = Vec::with_capacity(PAGE_WRITE_PAYLOAD_PREFIX + data.len());
    payload.extend_from_slice(&page_id.to_le_bytes());
    payload.extend_from_slice(&change_count.to_le_bytes());
    payload.extend_from_slice(data);
    self.append_record(RECORD_TYPE_PAGE_WRITE, &payload)?;

    Ok(PageLocation::Present { file_offset: data_offset, data_len })
  }

  fn append_page_tombstone(&mut self, page_id: u64, change_count: u64) -> anyhow::Result<()> {
    let mut payload = [0u8; 16];
    payload[..8].copy_from_slice(&page_id.to_le_bytes());
    payload[8..16].copy_from_slice(&change_count.to_le_bytes());
    self.append_record(RECORD_TYPE_PAGE_TOMBSTONE, &payload)
  }

  fn append_set_max_change_count(&mut self, change_count: u64) -> anyhow::Result<()> {
    self.append_record(RECORD_TYPE_SET_MAX_CHANGE_COUNT, &change_count.to_le_bytes())
  }

  fn append_set_writer_id(&mut self, writer_id: &str) -> anyhow::Result<()> {
    self.append_record(RECORD_TYPE_SET_WRITER_ID, writer_id.as_bytes())
  }

  fn append_delete_page(&mut self, page_id: u64) -> anyhow::Result<()> {
    self.append_record(RECORD_TYPE_DELETE_PAGE, &page_id.to_le_bytes())
  }

  fn append_commit_barrier(&mut self) -> anyhow::Result<()> {
    self.append_record(RECORD_TYPE_COMMIT_BARRIER, &[])
  }

  fn fdatasync(&self) -> anyhow::Result<()> {
    self.file.sync_data()?;
    Ok(())
  }
}

// -- Helpers --

fn compute_crc(rec_header: &[u8], payload: &[u8]) -> u32 {
  let mut hasher = crc32fast::Hasher::new();
  hasher.update(rec_header);
  hasher.update(payload);
  hasher.finalize()
}

fn open_reader(path: &Path) -> anyhow::Result<File> {
  File::open(path).map_err(|e| anyhow::anyhow!("failed to open WAL reader: {}", e))
}

/// Acquire a POSIX `flock(LOCK_EX | LOCK_NB)` on `file`.
///
/// The lock is advisory and held for the lifetime of the file descriptor.
/// A non-blocking attempt is used so that a clear error is reported
/// immediately if another process already holds the lock.
fn lock_exclusive(file: &File, path: &Path) -> anyhow::Result<()> {
  use std::os::unix::io::AsRawFd;
  let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
  if ret != 0 {
    let err = std::io::Error::last_os_error();
    anyhow::bail!(
      "failed to lock WAL file {:?} (another process may be using it): {}",
      path,
      err
    );
  }
  Ok(())
}

/// Read page data from the WAL using `pread(2)` (via `FileExt::read_exact_at`).
///
/// `pread` does not modify the file offset and is safe to call concurrently
/// with other `pread` calls and with sequential writes on a different fd.
fn read_page_data(reader: &File, file_offset: u64, data_len: u32) -> anyhow::Result<Bytes> {
  let mut buf = vec![0u8; data_len as usize];
  reader
    .read_exact_at(&mut buf, file_offset)
    .map_err(|e| anyhow::anyhow!("failed to read page data at offset {}: {}", file_offset, e))?;
  Ok(Bytes::from(buf))
}

fn wal_new_path(wal_path: &Path) -> PathBuf {
  let mut s = wal_path.as_os_str().to_owned();
  s.push(".new");
  PathBuf::from(s)
}

// -- WAL replay record types (index-only, no page data in memory) --

/// Intermediate index state without the reader handle (used during replay
/// before the WAL file is opened for reading).
#[derive(Default)]
struct WalIndexNoReader {
  pages: HashMap<u64, PageIndexEntry>,
  max_change_count: u64,
  writer_id: Option<ByteString>,
}

enum PendingIndexRecord {
  PageWrite { page_id: u64, change_count: u64, location: PageLocation },
  PageTombstone { page_id: u64, change_count: u64 },
  SetMaxChangeCount(u64),
  SetWriterId(ByteString),
  DeletePage(u64),
}

fn parse_index_record(
  record_type: u8,
  payload: &[u8],
  data_file_offset: u64,
) -> anyhow::Result<PendingIndexRecord> {
  match record_type {
    RECORD_TYPE_PAGE_WRITE => {
      if payload.len() < PAGE_WRITE_PAYLOAD_PREFIX {
        anyhow::bail!("PageWrite payload too short");
      }
      let page_id = u64::from_le_bytes(payload[..8].try_into().unwrap());
      let change_count = u64::from_le_bytes(payload[8..16].try_into().unwrap());
      let data_len = (payload.len() - PAGE_WRITE_PAYLOAD_PREFIX) as u32;
      Ok(PendingIndexRecord::PageWrite {
        page_id,
        change_count,
        location: PageLocation::Present { file_offset: data_file_offset, data_len },
      })
    }
    RECORD_TYPE_PAGE_TOMBSTONE => {
      if payload.len() != 16 {
        anyhow::bail!("PageTombstone payload wrong size");
      }
      let page_id = u64::from_le_bytes(payload[..8].try_into().unwrap());
      let change_count = u64::from_le_bytes(payload[8..16].try_into().unwrap());
      Ok(PendingIndexRecord::PageTombstone { page_id, change_count })
    }
    RECORD_TYPE_SET_MAX_CHANGE_COUNT => {
      if payload.len() != 8 {
        anyhow::bail!("SetMaxChangeCount payload wrong size");
      }
      Ok(PendingIndexRecord::SetMaxChangeCount(u64::from_le_bytes(
        payload.try_into().unwrap(),
      )))
    }
    RECORD_TYPE_SET_WRITER_ID => {
      let s = String::from_utf8(payload.to_vec())?;
      Ok(PendingIndexRecord::SetWriterId(ByteString::from(s)))
    }
    RECORD_TYPE_DELETE_PAGE => {
      if payload.len() != 8 {
        anyhow::bail!("DeletePage payload wrong size");
      }
      Ok(PendingIndexRecord::DeletePage(u64::from_le_bytes(
        payload.try_into().unwrap(),
      )))
    }
    _ => anyhow::bail!("unknown record type: 0x{:02x}", record_type),
  }
}

fn apply_pending_record(index: &mut WalIndexNoReader, record: PendingIndexRecord) {
  match record {
    PendingIndexRecord::PageWrite { page_id, change_count, location } => {
      index.pages.insert(page_id, PageIndexEntry { change_count, location });
    }
    PendingIndexRecord::PageTombstone { page_id, change_count } => {
      index.pages.insert(
        page_id,
        PageIndexEntry { change_count, location: PageLocation::Tombstone },
      );
    }
    PendingIndexRecord::SetMaxChangeCount(cc) => {
      index.max_change_count = cc;
    }
    PendingIndexRecord::SetWriterId(wid) => {
      index.writer_id = Some(wid);
    }
    PendingIndexRecord::DeletePage(page_id) => {
      index.pages.remove(&page_id);
    }
  }
}

// -- Public API --

/// Local write buffer for a single image, backed by an append-only WAL file.
///
/// # Architecture
///
/// Page data is **not** held in memory. The in-memory index (`WalIndex`)
/// maps `page_id → (change_count, file_offset, data_len)`. Reads are
/// served via `pread(2)` against the WAL file, which is typically hot in
/// the kernel page cache.
///
/// # Locking hierarchy (acquire in this order to avoid deadlock)
///
/// 1. `local_write_lock`  — tokio `Mutex<()>`, serialises write transactions
/// 2. `wal`               — `std::sync::Mutex<WalWriter>`, guards WAL appends
/// 3. `index`             — `parking_lot::RwLock<WalIndex>`, guards the index
///
/// Every code path that acquires more than one of these locks MUST acquire
/// them in the order 1 → 2 → 3. In particular, `wal` must always be
/// acquired before `index` write-lock. Read-locking `index` is permitted
/// at any time (it cannot deadlock with a write-lock from a different
/// thread because `parking_lot::RwLock` is fair/queued).
///
/// # File locking
///
/// The WAL file is `flock(LOCK_EX)`-ed on open. The lock is held for the
/// lifetime of the `WalWriter`'s file descriptor and prevents a second
/// `BufferStore` (in another process) from opening the same WAL.
pub struct BufferStore {
  index: Arc<RwLock<WalIndex>>,
  wal: Arc<std::sync::Mutex<WalWriter>>,
  wal_path: PathBuf,
  local_write_lock: Arc<Mutex<()>>,
}

/// A frozen, point-in-time snapshot of the buffer store for checkpoint
/// iteration.
///
/// Holds a clone of the page index (offsets only, no data) and an `Arc`
/// to the WAL reader at the time the snapshot was taken. Page data is read
/// from the WAL on demand during iteration.
pub struct BufferStoreSnapshot {
  pages: Vec<(u64, PageIndexEntry)>, // sorted by page_id
  change_count: u64,
  reader: Arc<File>,
}

/// A transaction against the buffer store.
///
/// Starts in read-only mode. Calling `lock_for_write` promotes it to a
/// write transaction (acquiring `local_write_lock`). Page writes are
/// buffered in `pending_writes` (in-memory, with actual `Bytes` data)
/// and flushed to the WAL atomically on `commit`.
///
/// # Correctness — atomicity
///
/// `pending_writes` is invisible to other readers until `commit` applies
/// them to the shared index under the `RwLock` write-lock. A dropped
/// (rolled-back) transaction simply discards `pending_writes`; no WAL
/// records are written.
///
/// # Correctness — conflict detection
///
/// `snapshot_change_count` is captured at `begin_txn` time.
/// `lock_for_write` compares it against the live `max_change_count`:
/// if another transaction committed in between, the counts differ and
/// the lock attempt returns `false` (conflict).
pub struct BufferStoreTxn {
  index: Arc<RwLock<WalIndex>>,
  wal: Arc<std::sync::Mutex<WalWriter>>,
  local_write_lock: Arc<Mutex<()>>,
  did_read: AtomicBool,
  snapshot_change_count: u64,
  write_state: Option<WriteTxnState>,
}

/// Pending write within an uncommitted transaction — data stays in memory
/// until commit, at which point it is written to the WAL and the index is
/// updated with the on-disk offset.
enum PendingWrite {
  Present(Bytes),
  Tombstone,
}

struct WriteTxnState {
  _guard: OwnedMutexGuard<()>,
  pending_writes: HashMap<u64, PendingWrite>,
}

impl BufferStore {
  pub fn new(buffer_store_path: PathBuf, image_id: ByteString) -> anyhow::Result<Self> {
    fs::create_dir_all(&buffer_store_path)?;
    let wal_path = buffer_store_path.join(format!("{}.wal", image_id));
    let (wal_writer, index) = WalWriter::open_or_create(&wal_path)?;

    Ok(Self {
      index: Arc::new(RwLock::new(index)),
      wal: Arc::new(std::sync::Mutex::new(wal_writer)),
      wal_path,
      local_write_lock: Arc::new(Mutex::new(())),
    })
  }

  pub fn get_writer_id(&self) -> anyhow::Result<Option<ByteString>> {
    Ok(self.index.read().writer_id.clone())
  }

  pub fn set_writer_id_if_not_exists(&self, writer_id: &ByteString) -> anyhow::Result<()> {
    let _guard = self
      .local_write_lock
      .try_lock()
      .map_err(|_| anyhow::anyhow!("local write lock held"))?;

    // Check under read lock first (cheap)
    if self.index.read().writer_id.is_some() {
      anyhow::bail!("writer id already exists");
    }

    // Acquire wal before index to maintain lock order: wal → index
    {
      let mut wal = self.wal.lock().unwrap();
      wal.append_set_writer_id(writer_id)?;
      wal.append_commit_barrier()?;
    }

    let reader = self.index.read().reader.clone();
    reader.sync_data()?;

    self.index.write().writer_id = Some(writer_id.clone());
    Ok(())
  }

  pub async fn advance_change_count(&self, target: u64) -> anyhow::Result<u64> {
    let _guard = self
      .local_write_lock
      .try_lock()
      .map_err(|_| anyhow::anyhow!("local write lock held"))?;

    let curr_change_count = self.index.read().max_change_count;
    if curr_change_count >= target {
      return Ok(curr_change_count);
    }
    if curr_change_count != 0 {
      anyhow::bail!(
        "local change count is non-zero but lags behind remote, corruption? local_change_count={}, target={}",
        curr_change_count,
        target
      );
    }

    {
      let mut wal = self.wal.lock().unwrap();
      wal.append_set_max_change_count(target)?;
      wal.append_commit_barrier()?;
    }
    self.index.read().reader.sync_data()?;

    self.index.write().max_change_count = target;
    Ok(curr_change_count)
  }

  pub async fn begin_txn(&self) -> anyhow::Result<BufferStoreTxn> {
    let snapshot_change_count = self.index.read().max_change_count;
    Ok(BufferStoreTxn {
      index: self.index.clone(),
      wal: self.wal.clone(),
      local_write_lock: self.local_write_lock.clone(),
      did_read: AtomicBool::new(false),
      snapshot_change_count,
      write_state: None,
    })
  }

  /// Take a point-in-time snapshot for checkpoint iteration.
  ///
  /// Clones the page index (offsets only — no page data is copied) and
  /// captures the current WAL reader handle so that iteration can read
  /// page data via `pread` even if a compaction swaps the reader later.
  pub fn snapshot_for_checkpoint(&self) -> anyhow::Result<BufferStoreSnapshot> {
    let index = self.index.read();
    let change_count = index.max_change_count;
    let reader = index.reader.clone();

    let mut pages: Vec<(u64, PageIndexEntry)> = index
      .pages
      .iter()
      .map(|(&page_id, entry)| (page_id, entry.clone()))
      .collect();
    pages.sort_by_key(|(page_id, _)| *page_id);

    Ok(BufferStoreSnapshot { pages, change_count, reader })
  }

  /// Remove pages from the index that were successfully checkpointed to S3.
  ///
  /// Only removes a page if its current `change_count` matches
  /// `expected_change_count` — if the page was modified after the checkpoint
  /// snapshot was taken, it is left in the buffer for the next checkpoint.
  ///
  /// # Cancellation safety
  ///
  /// The `OwnedMutexGuard` for `local_write_lock` is moved into the
  /// `spawn_blocking` closure. If the outer future is dropped, the closure
  /// (which tokio guarantees will run to completion) still holds the guard,
  /// preventing a concurrent writer from observing a half-updated index.
  pub async fn unbuffer_pages(&self, pages: Vec<(u64, u64)>) -> anyhow::Result<()> {
    let guard = self.local_write_lock.clone().lock_owned().await;

    let index = self.index.clone();
    let wal = self.wal.clone();

    spawn_blocking(move || {
      let _guard = guard;

      // Determine which pages to delete under a read lock (cheap).
      let deleted: Vec<u64> = {
        let index = index.read();
        pages
          .iter()
          .filter_map(|&(page_id, expected_change_count)| {
            index.pages.get(&page_id).and_then(|entry| {
              (entry.change_count == expected_change_count).then_some(page_id)
            })
          })
          .collect()
      };

      // Append to WAL (lock order: wal before index write-lock).
      {
        let mut wal = wal.lock().unwrap();
        for &page_id in &deleted {
          wal.append_delete_page(page_id)?;
        }
        wal.append_commit_barrier()?;
      }

      // fdatasync via the reader fd — flushes the same inode without
      // holding the wal mutex, so concurrent appends are not blocked.
      index.read().reader.sync_data()?;

      // Update the index last (lock order: index after wal).
      {
        let mut index = index.write();
        for &page_id in &deleted {
          index.pages.remove(&page_id);
        }
      }

      Ok::<_, anyhow::Error>(())
    })
    .await??;

    self.maybe_compact().await?;

    Ok(())
  }

  pub async fn len(&self) -> anyhow::Result<u64> {
    Ok(self.index.read().pages.len() as u64)
  }

  pub async fn fsync(&self) -> anyhow::Result<()> {
    let reader = self.index.read().reader.clone();
    spawn_blocking(move || {
      reader.sync_data().map_err(|e| anyhow::anyhow!("fsync failed: {}", e))
    })
    .await??;
    Ok(())
  }

  async fn maybe_compact(&self) -> anyhow::Result<()> {
    let wal_file_size = {
      let wal = self.wal.lock().unwrap();
      wal.file.metadata()?.len()
    };

    let estimated_live_size: u64 = {
      let index = self.index.read();
      index
        .pages
        .values()
        .map(|e| match &e.location {
          PageLocation::Present { data_len, .. } => {
            RECORD_HEADER_SIZE as u64 + PAGE_WRITE_PAYLOAD_PREFIX as u64 + *data_len as u64 + RECORD_TRAILER_SIZE as u64
          }
          PageLocation::Tombstone => {
            RECORD_HEADER_SIZE as u64 + 16 + RECORD_TRAILER_SIZE as u64
          }
        })
        .sum::<u64>()
        + WAL_HEADER_SIZE
        + 128
    };

    if wal_file_size > estimated_live_size * 2 && wal_file_size - estimated_live_size > 1024 * 1024 {
      self.compact().await?;
    }

    Ok(())
  }

  /// Rewrite the WAL file to contain only the live pages.
  ///
  /// # Correctness — crash safety
  ///
  /// 1. A new WAL is written to `<path>.wal.new`, then `fdatasync`-ed.
  /// 2. `rename` atomically replaces the old WAL.
  /// 3. The parent directory is `fsync`-ed to persist the rename.
  /// 4. The writer fd is re-opened and re-locked on the new file.
  /// 5. The index is updated with new offsets and a new reader handle.
  ///
  /// A crash at any point before step 2 leaves the old WAL intact; on
  /// restart, the stale `.wal.new` is deleted. A crash after step 2 but
  /// before step 3 is safe because the filesystem will resolve to either
  /// the old or new file — both are valid WALs.
  async fn compact(&self) -> anyhow::Result<()> {
    let guard = self.local_write_lock.clone().lock_owned().await;
    let index = self.index.clone();
    let wal = self.wal.clone();
    let wal_path = self.wal_path.clone();

    spawn_blocking(move || {
      let _guard = guard;

      // Read the current state under a read lock to build the new WAL.
      // We hold local_write_lock so no commits can change the index.
      let (old_reader, writer_id, max_cc, sorted_pages) = {
        let index = index.read();
        let old_reader = index.reader.clone();
        let writer_id = index.writer_id.clone();
        let max_cc = index.max_change_count;
        let mut sorted: Vec<(u64, PageIndexEntry)> = index
          .pages
          .iter()
          .map(|(&pid, e)| (pid, e.clone()))
          .collect();
        sorted.sort_by_key(|(pid, _)| *pid);
        (old_reader, writer_id, max_cc, sorted)
      };

      let new_path = wal_new_path(&wal_path);

      let new_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&new_path)?;

      let mut new_writer = WalWriter {
        file: new_file,
        next_seq: 0,
        position: WAL_HEADER_SIZE,
      };
      WalWriter::write_header(&mut new_writer.file)?;

      if let Some(ref wid) = writer_id {
        new_writer.append_set_writer_id(wid)?;
      }
      new_writer.append_set_max_change_count(max_cc)?;

      let mut new_entries: Vec<(u64, PageIndexEntry)> = Vec::with_capacity(sorted_pages.len());

      for (page_id, entry) in &sorted_pages {
        match &entry.location {
          PageLocation::Present { file_offset, data_len } => {
            let data = read_page_data(&old_reader, *file_offset, *data_len)?;
            let new_loc = new_writer.append_page_write(*page_id, entry.change_count, &data)?;
            new_entries.push((*page_id, PageIndexEntry {
              change_count: entry.change_count,
              location: new_loc,
            }));
          }
          PageLocation::Tombstone => {
            new_writer.append_page_tombstone(*page_id, entry.change_count)?;
            new_entries.push((*page_id, entry.clone()));
          }
        }
      }

      new_writer.append_commit_barrier()?;
      new_writer.fdatasync()?;

      // Atomic rename
      fs::rename(&new_path, &wal_path)?;

      // fsync parent directory
      if let Some(parent) = wal_path.parent() {
        if let Ok(dir) = File::open(parent) {
          let _ = dir.sync_all();
        }
      }

      // Lock order: wal before index.
      let mut wal_guard = wal.lock().unwrap();

      // Reopen the writer on the renamed file and re-lock
      let reopened = OpenOptions::new().read(true).write(true).open(&wal_path)?;
      lock_exclusive(&reopened, &wal_path)?;
      let end_pos = reopened.metadata()?.len();
      *wal_guard = WalWriter {
        file: reopened,
        next_seq: new_writer.next_seq,
        position: end_pos,
      };
      wal_guard.file.seek(SeekFrom::End(0))?;
      drop(wal_guard);

      let new_reader = Arc::new(open_reader(&wal_path)?);
      let mut index = index.write();
      index.pages = new_entries.into_iter().collect();
      index.reader = new_reader;

      Ok::<_, anyhow::Error>(())
    })
    .await??;

    Ok(())
  }
}

impl BufferStoreTxn {
  /// Read a page, checking (in order): pending writes → committed index → not present.
  ///
  /// For committed pages the data is read from the WAL file via `pread`.
  /// The `RwLock` read-lock on the index is held only long enough to copy
  /// the `PageLocation` and clone the `Arc<File>` reader — the actual I/O
  /// happens outside the lock.
  pub async fn read_page(&self, page_id: u64) -> anyhow::Result<PagePresence> {
    self.did_read.store(true, Ordering::Relaxed);

    if let Some(ws) = &self.write_state {
      if let Some(pw) = ws.pending_writes.get(&page_id) {
        return Ok(match pw {
          PendingWrite::Present(data) => {
            PagePresence::Present(DecompressedPage { data: data.clone() })
          }
          PendingWrite::Tombstone => PagePresence::Tombstone,
        });
      }
    }

    let (location, reader) = {
      let idx = self.index.read();
      match idx.pages.get(&page_id) {
        Some(entry) => (entry.location.clone(), idx.reader.clone()),
        None => return Ok(PagePresence::NotPresent),
      }
    };

    match location {
      PageLocation::Tombstone => Ok(PagePresence::Tombstone),
      PageLocation::Present { file_offset, data_len } => {
        let data = read_page_data(&reader, file_offset, data_len)?;
        Ok(PagePresence::Present(DecompressedPage { data }))
      }
    }
  }

  /// Promote this transaction to write mode by acquiring `local_write_lock`.
  ///
  /// Returns `false` (conflict) if another transaction committed since this
  /// transaction's `begin_txn`, detected by comparing `snapshot_change_count`
  /// with the current `max_change_count`. The caller must abort and retry.
  pub async fn lock_for_write(&mut self) -> anyhow::Result<bool> {
    if self.write_state.is_some() {
      return Ok(true);
    }

    let guard = self.local_write_lock.clone().lock_owned().await;

    if self.did_read.load(Ordering::Relaxed) {
      let curr_change_count = self.index.read().max_change_count;
      if curr_change_count != self.snapshot_change_count {
        return Ok(false);
      }
    }

    self.write_state = Some(WriteTxnState {
      _guard: guard,
      pending_writes: HashMap::new(),
    });
    Ok(true)
  }

  pub async fn write_page(&mut self, page_id: u64, data: Option<Bytes>) -> anyhow::Result<()> {
    if !self.lock_for_write().await? {
      anyhow::bail!("transaction cannot be locked");
    }

    if let Some(data) = &data {
      if data.is_empty() {
        anyhow::bail!("cannot write empty page");
      }
    }

    let ws = self.write_state.as_mut().unwrap();
    ws.pending_writes.insert(
      page_id,
      match data {
        Some(d) => PendingWrite::Present(d),
        None => PendingWrite::Tombstone,
      },
    );
    Ok(())
  }

  /// Flush all pending writes to the WAL and update the in-memory index.
  ///
  /// # Durability
  ///
  /// Records and a `CommitBarrier` are appended to the WAL, but `fdatasync`
  /// is **not** called automatically. The data is in the kernel page cache
  /// and will be visible to `pread` immediately, but is not guaranteed to
  /// survive a power loss until the caller invokes `BufferStore::fsync()`.
  /// This matches the architecture: `writeback_loop` calls `fsync()` before
  /// persisting image metadata to S3, establishing the durability boundary.
  ///
  /// Not syncing on every commit keeps `local_write_lock` hold-time short
  /// (microseconds for the in-memory index update rather than milliseconds
  /// for fdatasync), which is critical for avoiding write conflicts between
  /// concurrent connections to the same image.
  ///
  /// # Cancellation safety
  ///
  /// The `OwnedMutexGuard` for `local_write_lock` is moved into the
  /// `spawn_blocking` closure, ensuring the lock is held until WAL + index
  /// updates are complete even if the caller drops the future.
  pub async fn commit(self) -> anyhow::Result<()> {
    let Some(ws) = self.write_state else {
      return Ok(());
    };

    let WriteTxnState { _guard: guard, pending_writes: pending } = ws;

    let new_cc = self.snapshot_change_count + 1;
    let change_count = self.snapshot_change_count;
    let wal = self.wal.clone();
    let index = self.index.clone();

    spawn_blocking(move || {
      let _guard = guard;

      if pending.is_empty() {
        let mut wal = wal.lock().unwrap();
        wal.append_set_max_change_count(new_cc)?;
        wal.append_commit_barrier()?;
        drop(wal);
        index.write().max_change_count = new_cc;
        return Ok(());
      }

      let new_entries: Vec<(u64, PageIndexEntry)> = {
        let mut wal = wal.lock().unwrap();
        let mut entries = Vec::with_capacity(pending.len());

        for (&page_id, pw) in &pending {
          match pw {
            PendingWrite::Present(data) => {
              let loc = wal.append_page_write(page_id, change_count, data)?;
              entries.push((page_id, PageIndexEntry { change_count, location: loc }));
            }
            PendingWrite::Tombstone => {
              wal.append_page_tombstone(page_id, change_count)?;
              entries.push((
                page_id,
                PageIndexEntry { change_count, location: PageLocation::Tombstone },
              ));
            }
          }
        }

        wal.append_set_max_change_count(new_cc)?;
        wal.append_commit_barrier()?;
        entries
      };

      {
        let mut idx = index.write();
        for (page_id, entry) in new_entries {
          idx.pages.insert(page_id, entry);
        }
        idx.max_change_count = new_cc;
      }

      Ok::<_, anyhow::Error>(())
    })
    .await??;

    Ok(())
  }
}

impl BufferStoreSnapshot {
  pub fn iter(&self) -> anyhow::Result<BufferStoreSnapshotIterator<'_>> {
    Ok(BufferStoreSnapshotIterator {
      inner: self.pages.iter(),
      reader: &self.reader,
    })
  }

  pub fn change_count(&self) -> u64 {
    self.change_count
  }
}

/// Iterates over the snapshot, reading page data from the WAL via `pread` on
/// each call to `next`.
pub struct BufferStoreSnapshotIterator<'a> {
  inner: std::slice::Iter<'a, (u64, PageIndexEntry)>,
  reader: &'a File,
}

impl<'a> Iterator for BufferStoreSnapshotIterator<'a> {
  type Item = anyhow::Result<(u64, u64, Option<Bytes>)>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|(page_id, entry)| {
      match &entry.location {
        PageLocation::Tombstone => Ok((*page_id, entry.change_count, None)),
        PageLocation::Present { file_offset, data_len } => {
          let data = read_page_data(self.reader, *file_offset, *data_len)?;
          Ok((*page_id, entry.change_count, Some(data)))
        }
      }
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempdir::TempDir;

  fn create_bs(dir: &Path) -> BufferStore {
    BufferStore::new(dir.to_path_buf(), ByteString::from("test-image")).unwrap()
  }

  // --- basic read / write / commit ---

  #[tokio::test]
  async fn test_write_read_commit() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn = bs.begin_txn().await.unwrap();
    assert!(matches!(txn.read_page(0).await.unwrap(), PagePresence::NotPresent));

    txn.write_page(0, Some(Bytes::from_static(b"hello"))).await.unwrap();

    // Uncommitted write is visible to the writing transaction
    match txn.read_page(0).await.unwrap() {
      PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"hello")),
      other => panic!("expected Present, got {:?}", other),
    }

    // But not to a new transaction
    let txn2 = bs.begin_txn().await.unwrap();
    assert!(matches!(txn2.read_page(0).await.unwrap(), PagePresence::NotPresent));
    drop(txn2);

    txn.commit().await.unwrap();

    // After commit, visible to new transactions
    let txn3 = bs.begin_txn().await.unwrap();
    match txn3.read_page(0).await.unwrap() {
      PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"hello")),
      other => panic!("expected Present, got {:?}", other),
    }
  }

  // --- rollback (drop without commit) discards writes ---

  #[tokio::test]
  async fn test_rollback_discards_writes() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"discard me"))).await.unwrap();
    drop(txn); // rollback

    let txn = bs.begin_txn().await.unwrap();
    assert!(matches!(txn.read_page(0).await.unwrap(), PagePresence::NotPresent));
  }

  // --- tombstones ---

  #[tokio::test]
  async fn test_tombstone() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    // Write a page, commit
    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"data"))).await.unwrap();
    txn.commit().await.unwrap();

    // Delete it (tombstone), commit
    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, None).await.unwrap();
    txn.commit().await.unwrap();

    let txn = bs.begin_txn().await.unwrap();
    assert!(matches!(txn.read_page(0).await.unwrap(), PagePresence::Tombstone));
  }

  // --- overwrite updates data ---

  #[tokio::test]
  async fn test_overwrite() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"v1"))).await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"v2"))).await.unwrap();
    txn.commit().await.unwrap();

    let txn = bs.begin_txn().await.unwrap();
    match txn.read_page(0).await.unwrap() {
      PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"v2")),
      other => panic!("expected Present, got {:?}", other),
    }
  }

  // --- multiple pages in one transaction ---

  #[tokio::test]
  async fn test_multi_page_transaction() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn = bs.begin_txn().await.unwrap();
    for i in 0..100u64 {
      txn.write_page(i, Some(Bytes::from(vec![i as u8; 64]))).await.unwrap();
    }
    txn.commit().await.unwrap();

    let txn = bs.begin_txn().await.unwrap();
    for i in 0..100u64 {
      match txn.read_page(i).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from(vec![i as u8; 64])),
        other => panic!("page {} expected Present, got {:?}", i, other),
      }
    }
    assert!(matches!(txn.read_page(100).await.unwrap(), PagePresence::NotPresent));
  }

  // --- crash recovery: committed transaction survives reopen ---

  #[tokio::test]
  async fn test_recovery_committed_survives() {
    let dir = TempDir::new("wal-test").unwrap();

    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"durable"))).await.unwrap();
      txn.commit().await.unwrap();
    }

    // Reopen — simulates restart
    {
      let bs = create_bs(dir.path());
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"durable")),
        other => panic!("expected Present after recovery, got {:?}", other),
      }
    }
  }

  // --- crash recovery: uncommitted transaction is discarded ---

  #[tokio::test]
  async fn test_recovery_uncommitted_discarded() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test-image.wal");

    // Commit one transaction so the WAL exists
    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"committed"))).await.unwrap();
      txn.commit().await.unwrap();
    }

    // Append junk (simulate partial write without commit barrier)
    {
      let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
      f.write_all(b"partial-garbage-record").unwrap();
      f.sync_all().unwrap();
    }

    // Reopen — junk should be truncated
    {
      let bs = create_bs(dir.path());
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"committed")),
        other => panic!("expected Present, got {:?}", other),
      }
      // Only the committed page should exist
      assert!(matches!(txn.read_page(1).await.unwrap(), PagePresence::NotPresent));
    }
  }

  // --- snapshot for checkpoint ---

  #[tokio::test]
  async fn test_snapshot_iteration() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(2, Some(Bytes::from_static(b"page2"))).await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"page0"))).await.unwrap();
    txn.write_page(5, None).await.unwrap(); // tombstone
    txn.commit().await.unwrap();

    let snap = bs.snapshot_for_checkpoint().unwrap();
    assert_eq!(snap.change_count(), 1);

    let items: Vec<_> = snap.iter().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    // Sorted by page_id
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].0, 0);
    assert_eq!(items[0].2, Some(Bytes::from_static(b"page0")));
    assert_eq!(items[1].0, 2);
    assert_eq!(items[1].2, Some(Bytes::from_static(b"page2")));
    assert_eq!(items[2].0, 5);
    assert_eq!(items[2].2, None); // tombstone
  }

  // --- unbuffer_pages removes only matching change_count ---

  #[tokio::test]
  async fn test_unbuffer_pages() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    // T1: write pages 0, 1
    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"p0-v1"))).await.unwrap();
    txn.write_page(1, Some(Bytes::from_static(b"p1-v1"))).await.unwrap();
    txn.commit().await.unwrap();
    // change_count is now 1, pages have change_count=0

    // Snapshot for "checkpoint"
    let snap = bs.snapshot_for_checkpoint().unwrap();
    let page_ccs: Vec<(u64, u64)> = snap.iter().unwrap()
      .map(|r| { let (pid, cc, _) = r.unwrap(); (pid, cc) })
      .collect();
    assert_eq!(page_ccs, vec![(0, 0), (1, 0)]);

    // T2: overwrite page 0 (new change_count)
    let mut txn = bs.begin_txn().await.unwrap();
    txn.write_page(0, Some(Bytes::from_static(b"p0-v2"))).await.unwrap();
    txn.commit().await.unwrap();

    // Unbuffer with the snapshot's change_counts
    bs.unbuffer_pages(page_ccs).await.unwrap();

    // Page 0 should NOT have been removed (change_count changed from 0 → 1)
    // Page 1 SHOULD have been removed (change_count still 0)
    let txn = bs.begin_txn().await.unwrap();
    match txn.read_page(0).await.unwrap() {
      PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"p0-v2")),
      other => panic!("expected Present, got {:?}", other),
    }
    assert!(matches!(txn.read_page(1).await.unwrap(), PagePresence::NotPresent));
    assert_eq!(bs.len().await.unwrap(), 1);
  }

  // --- writer_id ---

  #[tokio::test]
  async fn test_writer_id() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    assert_eq!(bs.get_writer_id().unwrap(), None);

    let wid = ByteString::from("writer-1");
    bs.set_writer_id_if_not_exists(&wid).unwrap();
    assert_eq!(bs.get_writer_id().unwrap(), Some(wid.clone()));

    // Second call should fail
    assert!(bs.set_writer_id_if_not_exists(&ByteString::from("writer-2")).is_err());

    // Survives reopen
    drop(bs);
    let bs = create_bs(dir.path());
    assert_eq!(bs.get_writer_id().unwrap(), Some(wid));
  }

  // --- advance_change_count ---

  #[tokio::test]
  async fn test_advance_change_count() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let prev = bs.advance_change_count(10).await.unwrap();
    assert_eq!(prev, 0);

    // Already at or above target
    let prev = bs.advance_change_count(5).await.unwrap();
    assert_eq!(prev, 10);

    // Survives reopen
    drop(bs);
    let bs = create_bs(dir.path());
    let prev = bs.advance_change_count(10).await.unwrap();
    assert_eq!(prev, 10);
  }

  // --- conflict detection ---

  #[tokio::test]
  async fn test_conflict_detection_after_read() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn_a = bs.begin_txn().await.unwrap();
    let mut txn_b = bs.begin_txn().await.unwrap();

    // B reads a page — this sets did_read, enabling conflict detection
    let _ = txn_b.read_page(0).await.unwrap();

    // A commits first
    txn_a.write_page(0, Some(Bytes::from_static(b"from-a"))).await.unwrap();
    txn_a.commit().await.unwrap();

    // B tries to lock — should detect conflict because it read before A committed
    let locked = txn_b.lock_for_write().await.unwrap();
    assert!(!locked, "lock_for_write should return false on conflict after read");
  }

  #[tokio::test]
  async fn test_no_conflict_without_read() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn_a = bs.begin_txn().await.unwrap();
    let mut txn_b = bs.begin_txn().await.unwrap();

    // A commits first
    txn_a.write_page(0, Some(Bytes::from_static(b"from-a"))).await.unwrap();
    txn_a.commit().await.unwrap();

    // B did NOT read, so lock_for_write skips the change_count check
    let locked = txn_b.lock_for_write().await.unwrap();
    assert!(locked, "lock_for_write should succeed when no reads were done");
  }

  // --- file locking prevents double open ---

  #[test]
  fn test_file_lock_prevents_double_open() {
    let dir = TempDir::new("wal-test").unwrap();
    let _bs1 = create_bs(dir.path());

    // Second open of the same WAL should fail
    let result = BufferStore::new(dir.path().to_path_buf(), ByteString::from("test-image"));
    let err_msg = match result {
      Err(e) => format!("{}", e),
      Ok(_) => panic!("expected error, got Ok"),
    };
    assert!(err_msg.contains("lock"), "error should mention lock: {}", err_msg);
  }

  // --- recovery after multiple transactions ---

  #[tokio::test]
  async fn test_recovery_multiple_transactions() {
    let dir = TempDir::new("wal-test").unwrap();

    {
      let bs = create_bs(dir.path());
      for i in 0..5u64 {
        let mut txn = bs.begin_txn().await.unwrap();
        txn.write_page(i, Some(Bytes::from(vec![(i + 1) as u8; 128]))).await.unwrap();
        txn.commit().await.unwrap();
      }
      assert_eq!(bs.len().await.unwrap(), 5);
    }

    // Reopen and verify all pages
    {
      let bs = create_bs(dir.path());
      assert_eq!(bs.len().await.unwrap(), 5);
      let txn = bs.begin_txn().await.unwrap();
      for i in 0..5u64 {
        match txn.read_page(i).await.unwrap() {
          PagePresence::Present(p) => {
            assert_eq!(p.data, Bytes::from(vec![(i + 1) as u8; 128]));
          }
          other => panic!("page {} expected Present, got {:?}", i, other),
        }
      }
    }
  }

  // --- recovery with unbuffered pages ---

  #[tokio::test]
  async fn test_recovery_with_unbuffer() {
    let dir = TempDir::new("wal-test").unwrap();

    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"p0"))).await.unwrap();
      txn.write_page(1, Some(Bytes::from_static(b"p1"))).await.unwrap();
      txn.commit().await.unwrap();

      // Unbuffer page 1 (change_count=0)
      bs.unbuffer_pages(vec![(1, 0)]).await.unwrap();
      assert_eq!(bs.len().await.unwrap(), 1);
    }

    // Reopen — page 0 should be present, page 1 should not
    {
      let bs = create_bs(dir.path());
      assert_eq!(bs.len().await.unwrap(), 1);
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"p0")),
        other => panic!("expected Present, got {:?}", other),
      }
      assert!(matches!(txn.read_page(1).await.unwrap(), PagePresence::NotPresent));
    }
  }

  // --- large pages (simulates realistic 4 KiB block device pages) ---

  #[tokio::test]
  async fn test_large_pages() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let page_data: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();

    let mut txn = bs.begin_txn().await.unwrap();
    for i in 0..50u64 {
      let mut data = page_data.clone();
      data[0] = i as u8;
      txn.write_page(i, Some(Bytes::from(data))).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Verify via fresh transaction
    let txn = bs.begin_txn().await.unwrap();
    for i in 0..50u64 {
      let mut expected = page_data.clone();
      expected[0] = i as u8;
      match txn.read_page(i).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data[..], expected[..]),
        other => panic!("page {} expected Present, got {:?}", i, other),
      }
    }

    // Verify survives reopen
    drop(txn);
    drop(bs);
    let bs = create_bs(dir.path());
    let txn = bs.begin_txn().await.unwrap();
    for i in 0..50u64 {
      let mut expected = page_data.clone();
      expected[0] = i as u8;
      match txn.read_page(i).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data[..], expected[..]),
        other => panic!("page {} expected Present after reopen, got {:?}", i, other),
      }
    }
  }

  // --- empty commit (write lock acquired but no writes) ---

  #[tokio::test]
  async fn test_empty_commit_bumps_change_count() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());

    let mut txn = bs.begin_txn().await.unwrap();
    assert!(txn.lock_for_write().await.unwrap());
    txn.commit().await.unwrap();

    // change_count should have been bumped to 1
    let txn = bs.begin_txn().await.unwrap();
    assert_eq!(txn.snapshot_change_count, 1);
  }

  // --- CRC corruption is detected ---

  #[tokio::test]
  async fn test_crc_corruption_detected() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test-image.wal");

    // Write two valid transactions, then drop the BufferStore to release the flock
    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"valid"))).await.unwrap();
      txn.commit().await.unwrap();

      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(1, Some(Bytes::from_static(b"also-valid"))).await.unwrap();
      txn.commit().await.unwrap();
    } // BufferStore dropped here — flock released

    // Corrupt a byte in the middle of the file (second transaction)
    {
      let f = OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
      let len = f.metadata().unwrap().len();
      let corrupt_pos = WAL_HEADER_SIZE + (len - WAL_HEADER_SIZE) / 2;
      let mut byte = [0u8; 1];
      f.read_exact_at(&mut byte, corrupt_pos).unwrap();
      byte[0] ^= 0xFF;
      f.write_all_at(&byte, corrupt_pos).unwrap();
      f.sync_all().unwrap();
    }

    // Reopen — should recover first transaction, discard corrupted second
    {
      let bs = create_bs(dir.path());
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"valid")),
        other => panic!("expected first page to survive, got {:?}", other),
      }
      // Second page should be gone (its transaction was corrupted)
      assert!(matches!(txn.read_page(1).await.unwrap(), PagePresence::NotPresent));
    }
  }

  // --- fresh WAL on empty directory ---

  #[tokio::test]
  async fn test_fresh_wal_empty() {
    let dir = TempDir::new("wal-test").unwrap();
    let bs = create_bs(dir.path());
    assert_eq!(bs.len().await.unwrap(), 0);
    assert_eq!(bs.get_writer_id().unwrap(), None);

    let txn = bs.begin_txn().await.unwrap();
    assert!(matches!(txn.read_page(0).await.unwrap(), PagePresence::NotPresent));
    assert_eq!(txn.snapshot_change_count, 0);
  }

  // =========================================================================
  // Direct replay tests — exercise WalWriter::replay via hand-crafted WAL
  // files, verifying the returned (index, next_seq, valid_end) directly.
  // =========================================================================

  /// Write a raw WAL record into `file` at the current seek position.
  /// Returns the number of bytes written.
  fn write_raw_record(file: &mut File, record_type: u8, seq_no: u64, payload: &[u8]) -> u64 {
    let mut rec_header = [0u8; RECORD_HEADER_SIZE];
    rec_header[0] = record_type;
    rec_header[4..12].copy_from_slice(&seq_no.to_le_bytes());
    rec_header[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    let crc = compute_crc(&rec_header, payload);
    file.write_all(&rec_header).unwrap();
    file.write_all(payload).unwrap();
    file.write_all(&crc.to_le_bytes()).unwrap();
    RECORD_HEADER_SIZE as u64 + payload.len() as u64 + RECORD_TRAILER_SIZE as u64
  }

  fn write_raw_header(file: &mut File) {
    let mut header = [0u8; WAL_HEADER_SIZE as usize];
    header[..8].copy_from_slice(WAL_MAGIC);
    header[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
    file.write_all(&header).unwrap();
  }

  fn page_write_payload(page_id: u64, change_count: u64, data: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + data.len());
    p.extend_from_slice(&page_id.to_le_bytes());
    p.extend_from_slice(&change_count.to_le_bytes());
    p.extend_from_slice(data);
    p
  }

  fn page_tombstone_payload(page_id: u64, change_count: u64) -> Vec<u8> {
    let mut p = vec![0u8; 16];
    p[..8].copy_from_slice(&page_id.to_le_bytes());
    p[8..16].copy_from_slice(&change_count.to_le_bytes());
    p
  }

  // --- replay: empty WAL (header only) ---

  #[test]
  fn test_replay_empty_wal() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      f.sync_all().unwrap();
    }

    let (index, next_seq, valid_end) = WalWriter::replay(&wal_path).unwrap();
    assert_eq!(index.pages.len(), 0);
    assert_eq!(index.max_change_count, 0);
    assert_eq!(index.writer_id, None);
    assert_eq!(next_seq, 0);
    assert_eq!(valid_end, WAL_HEADER_SIZE);
  }

  // --- replay: single committed transaction ---

  #[test]
  fn test_replay_single_committed_txn() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut pos = WAL_HEADER_SIZE;
      let mut seq = 0u64;

      pos += write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"hello"));
      seq += 1;
      pos += write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(1, 0, b"world"));
      seq += 1;
      pos += write_raw_record(&mut f, RECORD_TYPE_SET_MAX_CHANGE_COUNT, seq, &1u64.to_le_bytes());
      seq += 1;
      pos += write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      seq += 1;

      f.sync_all().unwrap();

      let (index, next_seq, valid_end) = WalWriter::replay(&wal_path).unwrap();
      assert_eq!(index.pages.len(), 2);
      assert_eq!(index.max_change_count, 1);
      assert_eq!(next_seq, seq);
      assert_eq!(valid_end, pos);
    }
  }

  // --- replay: uncommitted trailing records are discarded ---

  #[test]
  fn test_replay_trailing_uncommitted_discarded() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut pos = WAL_HEADER_SIZE;
      let mut seq = 0u64;

      // First transaction — committed
      pos += write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"committed"));
      seq += 1;
      pos += write_raw_record(&mut f, RECORD_TYPE_SET_MAX_CHANGE_COUNT, seq, &1u64.to_le_bytes());
      seq += 1;
      pos += write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      seq += 1;
      let committed_end = pos;

      // Second transaction — no commit barrier
      pos += write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(1, 1, b"uncommitted"));
      seq += 1;
      pos += write_raw_record(&mut f, RECORD_TYPE_SET_MAX_CHANGE_COUNT, seq, &2u64.to_le_bytes());
      seq += 1;
      // No COMMIT_BARRIER

      f.sync_all().unwrap();
      let _ = (pos, seq); // suppress unused warnings

      let (index, next_seq, valid_end) = WalWriter::replay(&wal_path).unwrap();
      assert_eq!(index.pages.len(), 1);
      assert!(index.pages.contains_key(&0));
      assert!(!index.pages.contains_key(&1));
      assert_eq!(index.max_change_count, 1);
      assert_eq!(valid_end, committed_end);
      // next_seq must be the seq after the last COMMITTED barrier (3),
      // not the total records scanned (5) — otherwise new writes would
      // use inflated seq numbers that break the next recovery.
      assert_eq!(next_seq, 3);
    }
  }

  // --- replay: valid_end is used to truncate the file on reopen ---

  #[tokio::test]
  async fn test_replay_truncates_to_valid_end() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test-image.wal");

    // Manually create a WAL with a committed txn followed by an uncommitted one
    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"keep"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_SET_MAX_CHANGE_COUNT, seq, &1u64.to_le_bytes());
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      seq += 1;

      // Uncommitted trailing data
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(1, 1, b"discard"));
      seq += 1;
      let _ = seq;
      f.sync_all().unwrap();
    }

    let size_before = fs::metadata(&wal_path).unwrap().len();

    // Open (triggers replay + truncation)
    {
      let bs = create_bs(dir.path());
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data[..], b"keep"[..]),
        other => panic!("expected Present, got {:?}", other),
      }
      assert!(matches!(txn.read_page(1).await.unwrap(), PagePresence::NotPresent));
    }

    let size_after = fs::metadata(&wal_path).unwrap().len();
    assert!(size_after < size_before, "file should have been truncated: before={}, after={}", size_before, size_after);
  }

  // --- replay: sequence number gap is detected ---

  #[test]
  fn test_replay_seq_gap_detected() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      // First transaction — committed
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"ok"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      seq += 1;

      // Second transaction — skip seq_no (write seq 3 instead of 2)
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq + 1, &page_write_payload(1, 0, b"bad-seq"));

      f.sync_all().unwrap();
      let _ = seq;
    }

    let (index, _next_seq, _valid_end) = WalWriter::replay(&wal_path).unwrap();
    // Only the first transaction should survive
    assert_eq!(index.pages.len(), 1);
    assert!(index.pages.contains_key(&0));
    assert!(!index.pages.contains_key(&1));
  }

  // --- replay: tombstones survive reopen ---

  #[tokio::test]
  async fn test_replay_tombstone_survives_reopen() {
    let dir = TempDir::new("wal-test").unwrap();

    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"data"))).await.unwrap();
      txn.write_page(1, None).await.unwrap(); // tombstone
      txn.commit().await.unwrap();
    }

    // Reopen and verify tombstone survived replay
    {
      let bs = create_bs(dir.path());
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"data")),
        other => panic!("expected Present, got {:?}", other),
      }
      assert!(matches!(txn.read_page(1).await.unwrap(), PagePresence::Tombstone));
    }
  }

  // --- replay: page data offsets are correct after replay ---

  #[test]
  fn test_replay_page_offsets_correct() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    let data_a = b"aaaa-page-data";
    let data_b = b"bbbb-page-data-longer";

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(10, 0, data_a));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(20, 0, data_b));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      let _ = seq;
      f.sync_all().unwrap();
    }

    let (index, _, _) = WalWriter::replay(&wal_path).unwrap();
    let reader = File::open(&wal_path).unwrap();

    // Verify page 10
    let entry_a = index.pages.get(&10).unwrap();
    match &entry_a.location {
      PageLocation::Present { file_offset, data_len } => {
        assert_eq!(*data_len as usize, data_a.len());
        let data = read_page_data(&reader, *file_offset, *data_len).unwrap();
        assert_eq!(data[..], data_a[..]);
      }
      PageLocation::Tombstone => panic!("expected Present"),
    }

    // Verify page 20
    let entry_b = index.pages.get(&20).unwrap();
    match &entry_b.location {
      PageLocation::Present { file_offset, data_len } => {
        assert_eq!(*data_len as usize, data_b.len());
        let data = read_page_data(&reader, *file_offset, *data_len).unwrap();
        assert_eq!(data[..], data_b[..]);
      }
      PageLocation::Tombstone => panic!("expected Present"),
    }
  }

  // --- replay: overwrite within same transaction takes last value ---

  #[test]
  fn test_replay_overwrite_within_txn() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"first"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"second"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      let _ = seq;
      f.sync_all().unwrap();
    }

    let (index, _, _) = WalWriter::replay(&wal_path).unwrap();
    let reader = File::open(&wal_path).unwrap();

    let entry = index.pages.get(&0).unwrap();
    match &entry.location {
      PageLocation::Present { file_offset, data_len } => {
        let data = read_page_data(&reader, *file_offset, *data_len).unwrap();
        assert_eq!(data[..], b"second"[..]);
      }
      PageLocation::Tombstone => panic!("expected Present"),
    }
  }

  // --- replay: DeletePage record removes a page ---

  #[test]
  fn test_replay_delete_page() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      // T1: write page 0 and 1
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(0, 0, b"p0"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(1, 0, b"p1"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      seq += 1;

      // T2: delete page 1
      write_raw_record(&mut f, RECORD_TYPE_DELETE_PAGE, seq, &1u64.to_le_bytes());
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      let _ = seq;
      f.sync_all().unwrap();
    }

    let (index, _, _) = WalWriter::replay(&wal_path).unwrap();
    assert_eq!(index.pages.len(), 1);
    assert!(index.pages.contains_key(&0));
    assert!(!index.pages.contains_key(&1));
  }

  // --- replay: writer_id and change_count are recovered ---

  #[test]
  fn test_replay_metadata() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      write_raw_record(&mut f, RECORD_TYPE_SET_WRITER_ID, seq, b"writer-42");
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_SET_MAX_CHANGE_COUNT, seq, &7u64.to_le_bytes());
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      let _ = seq;
      f.sync_all().unwrap();
    }

    let (index, _, _) = WalWriter::replay(&wal_path).unwrap();
    assert_eq!(index.writer_id, Some(ByteString::from("writer-42")));
    assert_eq!(index.max_change_count, 7);
  }

  // --- replay: tombstone in replay index ---

  #[test]
  fn test_replay_tombstone_in_index() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test.wal");

    {
      let mut f = File::create(&wal_path).unwrap();
      write_raw_header(&mut f);
      let mut seq = 0u64;

      write_raw_record(&mut f, RECORD_TYPE_PAGE_TOMBSTONE, seq, &page_tombstone_payload(5, 0));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_COMMIT_BARRIER, seq, &[]);
      let _ = seq;
      f.sync_all().unwrap();
    }

    let (index, _, _) = WalWriter::replay(&wal_path).unwrap();
    assert_eq!(index.pages.len(), 1);
    let entry = index.pages.get(&5).unwrap();
    assert!(matches!(entry.location, PageLocation::Tombstone));
    assert_eq!(entry.change_count, 0);
  }

  // --- replay: stale .wal.new is cleaned up ---

  #[tokio::test]
  async fn test_replay_cleans_stale_wal_new() {
    let dir = TempDir::new("wal-test").unwrap();

    // Create a valid WAL
    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"data"))).await.unwrap();
      txn.commit().await.unwrap();
    }

    // Create a stale .wal.new file (simulates interrupted compaction)
    let new_path = dir.path().join("test-image.wal.new");
    fs::write(&new_path, b"stale compaction file").unwrap();
    assert!(new_path.exists());

    // Reopen — should clean up .wal.new and work fine
    {
      let bs = create_bs(dir.path());
      assert!(!new_path.exists(), ".wal.new should have been deleted");
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data, Bytes::from_static(b"data")),
        other => panic!("expected Present, got {:?}", other),
      }
    }
  }

  // --- regression: committed data must survive two recoveries with trailing
  //     uncommitted records (the seq-number gap bug) ---

  #[tokio::test]
  async fn test_recovery_seq_gap_regression() {
    let dir = TempDir::new("wal-test").unwrap();
    let wal_path = dir.path().join("test-image.wal");

    // Phase 1: commit page 0
    {
      let bs = create_bs(dir.path());
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(0, Some(Bytes::from_static(b"phase1"))).await.unwrap();
      txn.commit().await.unwrap();
    }

    // Simulate crash with trailing uncommitted records
    {
      let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
      // Write two well-formed records WITHOUT a commit barrier. The records
      // have valid CRCs so replay will scan past them before noticing the
      // missing barrier.
      let file_len = f.metadata().unwrap().len();
      // The committed WAL has some number of records. We need to figure out
      // the next expected seq. Replay the file to find out.
      let (_, committed_next_seq, _) = WalWriter::replay(&wal_path).unwrap();
      let mut seq = committed_next_seq;
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(99, 1, b"uncommitted-a"));
      seq += 1;
      write_raw_record(&mut f, RECORD_TYPE_PAGE_WRITE, seq, &page_write_payload(98, 1, b"uncommitted-b"));
      let _ = seq;
      f.sync_all().unwrap();
      assert!(f.metadata().unwrap().len() > file_len, "trailing records should grow the file");
    }

    // Phase 2: reopen (first recovery — truncates trailing records), commit page 1
    {
      let bs = create_bs(dir.path());
      // Page 0 from phase 1 should survive
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data[..], b"phase1"[..]),
        other => panic!("phase1 page should survive first recovery, got {:?}", other),
      }
      // Uncommitted pages should be gone
      assert!(matches!(txn.read_page(99).await.unwrap(), PagePresence::NotPresent));
      drop(txn);

      // Write new data
      let mut txn = bs.begin_txn().await.unwrap();
      txn.write_page(1, Some(Bytes::from_static(b"phase2"))).await.unwrap();
      txn.commit().await.unwrap();
    }

    // Phase 3: reopen again (second recovery) — BOTH pages must survive.
    // Before the fix, the seq gap caused phase2's records to be rejected
    // on this second recovery, silently losing page 1.
    {
      let bs = create_bs(dir.path());
      let txn = bs.begin_txn().await.unwrap();
      match txn.read_page(0).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data[..], b"phase1"[..]),
        other => panic!("phase1 page should survive second recovery, got {:?}", other),
      }
      match txn.read_page(1).await.unwrap() {
        PagePresence::Present(p) => assert_eq!(p.data[..], b"phase2"[..]),
        other => panic!("phase2 page should survive second recovery, got {:?}", other),
      }
    }
  }
}
