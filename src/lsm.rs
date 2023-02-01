//! LSM Module.

use std::collections::BTreeMap;
use std::fs;

use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock};

use crate::config::Config;
use crate::disk::format::DiskEntry;
use crate::disk::sstable::SSTable;
use crate::disk::wal::WAL;
use crate::error::Result;
use crate::storage::{Storage, Store};
use crate::utils;
use crate::worker::compact::{Compactor, CompactorMessage};

/// KVStore API definitions.
pub trait KVStore {
    /// Put a key/value pair into the store.
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Delete a key/value pair from the store.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Get a key/value pair from the store.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Contains a key/value pair in the store or not.
    fn contains(&self, key: &[u8]) -> bool;

    /// List all keys in the store.
    fn list_keys(&self) -> Result<Vec<Vec<u8>>>;
}

/// Lsm handler.
pub struct Lsm {
    /// Path of the datastore.
    path: PathBuf,

    /// Disk Storage handler.
    store: Arc<RwLock<Store>>,

    /// OutBox for sync message with compactor.
    worker_outbox: mpsc::Sender<CompactorMessage>,

    /// MemTable of the key/value pair.
    /// use for read first, update write, sorted.
    /// memtable: MemTable,
    memtable: BTreeMap<Vec<u8>, DiskEntry>,

    /// wal for memtable crushed.
    log: WAL,

    /// dirty_bytes.
    dirty_bytes: u64,

    /// config of store.
    config: Config,
    //// stats.
    //// stats: Stats,
}

pub struct OpenOptions(Config);

impl OpenOptions {
    pub fn new() -> Self {
        Self(Config::default())
    }

    pub fn max_space_amp(mut self, value: u8) -> Self {
        self.0.max_space_amp = value;
        self
    }

    pub fn max_log_length(mut self, value: u64) -> Self {
        self.0.max_log_length = value;
        self
    }

    pub fn merge_ratio(mut self, value: u8) -> Self {
        self.0.merge_ratio = value;
        self
    }

    pub fn merge_window(mut self, value: u8) -> Self {
        self.0.merge_window = value;
        self
    }

    pub fn log_bufwriter_size(mut self, value: u32) -> Self {
        self.0.log_bufwriter_size = value;
        self
    }

    pub fn zstd_sstable_compression_level(mut self, value: u8) -> Self {
        self.0.zstd_sstable_compression_level = value;
        self
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<Lsm> {
        Lsm::open_with_options(path, self.0.clone())
    }
}

impl Lsm {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Config::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path = path.as_ref();

        let store = Store::open_with_options(path, config)?;
        let sstables = store.list_sstables();

        let store = Arc::new(RwLock::new(store));

        // build memtable from WAL.
        let (log, memtable, dirty_bytes) = Self::build_memtable(path)?;

        // create worker message channel.
        let (tx, rx) = mpsc::channel();
        // let worker_stats = Arc::new(WorkerStats::new());
        let worker = Compactor {
            path: path.to_path_buf(),
            sstables,
            store: Arc::clone(&store),
            inbox: rx,
            config: config.clone(),
        };

        std::thread::spawn(move || worker.run());

        let (hb_tx, hb_rx) = mpsc::channel();
        tx.send(CompactorMessage::HeartBeat(hb_tx)).unwrap();

        for _ in hb_rx {}

        log::info!("config: {:?}", config);

        Ok(Self {
            path: path.to_path_buf(),
            store: store.clone(),
            memtable,
            log,
            dirty_bytes,
            config,
            worker_outbox: tx,
            // stats: Stats::default(),
        })
    }

    /// Create or Recover memtable
    fn build_memtable(path: &Path) -> Result<(SSTable, BTreeMap<Vec<u8>, DiskEntry>, u64)> {
        let path = utils::format_wal_path(path, 0);

        log::info!("recover memtable from log {}", path.display());

        let mut log = WAL::new(path, true)?;

        let mut memtable = BTreeMap::new();
        let mut recoverd = 0u64;

        for entry in log.iter() {
            let (crc_expected, crc_actual) = (entry.crc_expected(), entry.crc_actual());
            if crc_actual != crc_expected {
                log::warn!(
                    "crc mismatch for kv pairs {:?}-{:?} expected {} actual {}, torn log detected",
                    entry.key,
                    entry.value,
                    crc_expected,
                    crc_actual,
                );
                break;
            }

            recoverd += entry.size();

            memtable.insert(entry.key.clone(), entry);
        }

        // truncate log file.
        if log.size() > recoverd {
            log.truncate(recoverd)?;
        }

        // need to back up a few bytes to chop off the torn log.
        log::debug!("recoverd {} kv pairs", memtable.len());
        log::debug!("rewinding log down to length {}", recoverd);

        Ok((log, memtable, recoverd))
    }

    fn log_mutation(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // first: record log.
        let disk_entry = self.log.write(&key, &value)?;
        self.dirty_bytes += disk_entry.size();

        // then: insert memory.
        self.memtable.insert(key, disk_entry);

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        log::info!("flush start...");

        // WAL sync and flush.
        self.log.sync()?;

        if self.dirty_bytes > self.config.max_log_length {
            log::debug!("compacting log to new sstable...");
            let memtable = std::mem::take(&mut self.memtable);

            let sstable = self.store.write().unwrap().set(&memtable);

            if let Err(e) = sstable {
                // put memtable back together before returning
                self.memtable = memtable;

                log::error!("failed to flush memtable to sstable, error: {}", e);
                return Err(e.into());
            }

            let (next_sstable_id, size) = sstable.unwrap();

            // Send message to worker, it may trigger compacting.
            if let Err(e) = self.worker_outbox.send(CompactorMessage::NewSSTable {
                id: next_sstable_id,
                size: size,
            }) {
                log::error!("failed to send message to worker: {:?}", e);
                log::logger().flush();
                panic!("failed to send message to worker: {:?}", e);
            }

            // truncate log file.
            self.log.truncate(0)?;
            fs::File::open(&self.path)?.sync_all()?;

            self.dirty_bytes = 0;

            log::info!("created sstable: {} size: {}", next_sstable_id, size);
        }

        Ok(())
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        let (tx, rx) = mpsc::channel();

        if self.worker_outbox.send(CompactorMessage::Stop(tx)).is_err() {
            log::error!("failed to shutdown compaction worker on Lsm drop");
            return;
        }

        // #[cfg(test)]
        // assert!(!self.worker.tick());

        for _ in rx {}
    }
}

impl KVStore for Lsm {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.log_mutation(key, value)?;

        // log::info!("dirty_bytes: {:?}", self.dirty_bytes);

        // rotate log and flush memtable to disk.
        if self.dirty_bytes > self.config.max_log_length {
            self.flush()?;
        }

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.contains(key) {
            log::trace!(
                "remove key: `{}`, but it not found in database",
                String::from_utf8_lossy(key)
            );
            return Ok(());
        }

        self.put(key.to_vec(), Vec::new())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(entry) = self.memtable.get(key) {
            if entry.value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(entry.value.clone()));
        } else {
            self.store.write().unwrap().get(key)
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        // first: check memtable.
        if self.memtable.contains_key(key) {
            return true;
        }
        // then: check keydir.
        self.store.read().unwrap().contains_key(key)
    }

    fn list_keys(&self) -> Result<Vec<Vec<u8>>> {
        let mut keys = self.store.read().unwrap().keys()?;
        keys.sort();

        self.memtable.iter().for_each(|v| {
            if !v.1.value.is_empty() {
                keys.push(v.0.clone());
            } else {
                if let Ok(index) = keys.binary_search(v.0) {
                    keys.remove(index);
                }
            }
        });

        Ok(keys)
    }
}
