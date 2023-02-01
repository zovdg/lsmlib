//! Storage Module.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::config::{self, Config};
use crate::disk::format::DiskEntry;
use crate::disk::{format::HintEntry, hint::HintFile, sstable::SSTable};
use crate::error::{LSMLibError, Result};
use crate::keydir::{HashmapKeydir, Keydir, KeydirEntry};
use crate::utils;

pub type Store = DiskStorage<HashmapKeydir>;

/// Store implementation methods.
pub trait Storage {
    /// Get value by key from the store.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Flush memtable to sstable file in store.
    fn set(&mut self, items: &BTreeMap<Vec<u8>, DiskEntry>) -> Result<(u64, u64)>;

    /// List all keys in the store.
    fn keys(&self) -> Result<Vec<Vec<u8>>>;

    /// Return total number of keys in datastore.
    fn len(&self) -> u64;

    /// Check datastore is empty or not.
    fn is_empty(&self) -> bool;

    /// Return `true` if datastore contains the given key.
    fn contains_key(&self, key: &[u8]) -> bool;

    /// Iterate all keys in datastore and call function `f`
    /// for each entry.
    ///
    /// If function `f` return an `Err`, it stops iteration
    /// and propagates the `Err` to the caller.
    ///
    /// You can continue iteration manually by returning `Ok(true)`,
    /// or stop iteration by returning `Ok(false)`.
    fn for_each<F>(&self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>;

    /// Force flushing any pending writes to the datastore.
    fn flush(&mut self) -> Result<()>;
}

/// Keydir update methods.
pub trait KeydirUpdate {
    fn compact_and_merge(&mut self, sstable_ids: &[u64]) -> Result<(u64, u64)>;
}

/// A simple lockfile for `DistStorage`.
#[derive(Debug)]
pub struct Lockfile {
    handle: Option<File>,
    path: PathBuf,
}

impl Lockfile {
    /// Creates a lock at the provided `path`. Fails if lock is already exists.
    pub fn lock(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let dir_path = path.parent().expect("lock file must have a parent");
        fs::create_dir_all(dir_path)?;

        let mut lockfile_opts = fs::OpenOptions::new();
        lockfile_opts.read(true).write(true).create_new(true);

        let lockfile = lockfile_opts.open(path)?;

        Ok(Self {
            handle: Some(lockfile),
            path: path.to_path_buf(),
        })
    }
}

impl Drop for Lockfile {
    fn drop(&mut self) {
        self.handle.take();
        fs::remove_file(&self.path).expect("lock already dropped.");
    }
}

/// Disk storage.
pub struct DiskStorage<K>
where
    K: Keydir + Default,
{
    /// directory for datastore.
    path: PathBuf,

    /// lock for database directory.
    _lock: Lockfile,

    /// holds a bunch of sstable files.
    sstables: BTreeMap<u64, SSTable>,

    /// Keydir maintains key value index for fast query.
    keydir: K,

    /// config options.
    config: Config,
}

impl<K> Drop for DiskStorage<K>
where
    K: Keydir + Default,
{
    fn drop(&mut self) {
        // ignore sync errors.
        log::trace!("sync all pending writes to disk.");
        //let _r = self.sync();
    }
}

impl<K> DiskStorage<K>
where
    K: Keydir + Default,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Config::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path = path.as_ref();

        log::info!("open store path: {}", path.display());

        fs::create_dir_all(path)?;
        fs::File::open(path)?.flush()?; // sync to disk.

        let lock = Lockfile::lock(path.join("LOCK")).or(Err(LSMLibError::AlreadyLocked))?;

        let mut store = Self {
            path: path.to_path_buf(),
            _lock: lock,
            sstables: BTreeMap::new(),
            keydir: K::default(),
            config,
        };

        store.open_sstables()?;
        store.build_keydir()?;

        Ok(store)
    }

    pub fn list_sstables(&self) -> BTreeMap<u64, u64> {
        self.sstables.iter().map(|s| (*s.0, s.1.size())).collect()
    }

    /// Open sstable files(they are immutable).
    fn open_sstables(&mut self) -> Result<()> {
        let pattern = format!("{}/*{}", self.path.display(), config::DATA_FILE_SUFFIX);
        log::trace!("read sstable files with pattern {}", &pattern);

        for path in glob::glob(&pattern)? {
            let sst = SSTable::new(path?.as_path(), false)?;

            self.sstables.insert(sst.id(), sst);
        }
        log::trace!("got {} immutable sstable files", self.sstables.len());

        Ok(())
    }

    /// Build keydir index from sstable or it's hint.
    fn build_keydir(&mut self) -> Result<()> {
        let mut file_ids: Vec<u64> = self.sstables.keys().cloned().collect();
        file_ids.sort();

        for file_id in file_ids {
            let hint_file_path = utils::format_hint_path(&self.path, file_id);
            if hint_file_path.exists() {
                self.build_keydir_from_hint(hint_file_path.as_path())?;
            } else {
                self.build_keydir_from_sstable(file_id)?;
            }
        }

        log::info!("build keydir done, got {} keys", self.keydir.len());

        Ok(())
    }

    fn build_keydir_from_hint(&mut self, path: &Path) -> Result<()> {
        log::trace!("build keydir from hint file {}", path.display());
        let mut hint_file = HintFile::new(path, false)?;
        let _hint_file_id = hint_file.id();

        for entry in hint_file.iter() {
            if entry.value_sz() != 0 {
                let keydir_entry = KeydirEntry::try_from(&entry)?;
                self.keydir.put(entry.key, keydir_entry);
            } else {
                self.keydir.remove(&entry.key);
            }
        }

        Ok(())
    }

    fn build_keydir_from_sstable(&mut self, file_id: u64) -> Result<()> {
        let sst = self.sstables.get_mut(&file_id).unwrap();
        log::info!("build keydir from data file {}", sst.path().display());

        for entry in sst.iter() {
            if entry.value.is_empty() {
                log::trace!("{} is a remove tomestone", &entry);

                self.keydir.remove(&entry.key);
                continue;
            }
            let keydir_entry = KeydirEntry::try_from(&entry)?;
            let _ = self.keydir.put(entry.key, keydir_entry);
        }

        Ok(())
    }
}

impl<K> Storage for DiskStorage<K>
where
    K: Keydir + Default,
{
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(keydir_entry) = self.keydir.get(&key) {
            log::trace!(
                "found key `{}` in keydir, got value `{:?}`",
                String::from_utf8_lossy(key),
                &keydir_entry,
            );

            let sst = self
                .sstables
                .get_mut(&keydir_entry.file_id)
                .unwrap_or_else(|| {
                    panic!("sstable file `{}` not found", keydir_entry.file_id);
                });

            if let Some(disk_entry) = sst.read(keydir_entry.offset)? {
                return Ok(disk_entry.value.into());
            }
        }

        Ok(None)
    }

    fn set(&mut self, items: &BTreeMap<Vec<u8>, DiskEntry>) -> Result<(u64, u64)> {
        let next_sstable_id = self.sstables.keys().max().copied().unwrap_or(0) + 1;

        let sstable_path = utils::format_sstable_path(&self.path, next_sstable_id);
        let hint_path = utils::format_hint_path(&self.path, next_sstable_id);

        let mut sstable = SSTable::new(&sstable_path, true)?;
        let mut hint = HintFile::new(&hint_path, true)?;

        for (k, entry) in items {
            // write sstable file.
            let disk_entry = sstable.write_entry(entry.clone())?;

            // write hint file.
            hint.write_entry(HintEntry::from(&disk_entry))?;

            // not hint
            if disk_entry.value.is_empty() {
                self.keydir.remove(&k);
            } else {
                // update keydir.
                self.keydir
                    .put(k.to_vec(), KeydirEntry::try_from(&disk_entry)?);
            }
        }

        sstable.sync()?;
        hint.sync()?;

        self.sstables
            .insert(next_sstable_id, SSTable::new(&sstable_path, false)?);

        Ok((next_sstable_id, sstable.size()))
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.keydir.contains_key(key)
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        Ok(self.keydir.keys())
    }

    fn len(&self) -> u64 {
        self.keydir.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn for_each<F>(&self, _f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

impl<K> KeydirUpdate for DiskStorage<K>
where
    K: Keydir + Default,
{
    fn compact_and_merge(&mut self, sstable_ids: &[u64]) -> Result<(u64, u64)> {
        log::debug!(
            "start do keydir updating for compact and merge sstable_ids: {:?}",
            sstable_ids
        );

        let max_sstable_id = sstable_ids
            .iter()
            .max()
            .copied()
            .expect("compact_sstable_run called with empty set of sst ids");

        let merge_tmp_path = utils::format_sstable_tmp_path(&self.path, max_sstable_id);
        let merge_hint_tmp_path = utils::format_hint_tmp_path(&self.path, max_sstable_id);

        let merge_path = utils::format_sstable_path(&self.path, max_sstable_id);
        let merge_hint_path = utils::format_hint_path(&self.path, max_sstable_id);

        fs::rename(&merge_tmp_path, &merge_path)?;
        fs::rename(&merge_hint_tmp_path, &merge_hint_path)?;
        fs::File::open(&self.path)?.flush()?; // sync to disk.

        for sstable_id in sstable_ids {
            if max_sstable_id == *sstable_id {
                continue;
            }

            // remove compacted sstable file.
            let path = utils::format_sstable_path(&self.path, *sstable_id);
            fs::remove_file(path)?;

            self.sstables
                .remove(sstable_id)
                .expect("compacted sstable not persent in sstables");

            // remove compacted hint file.
            let hint_path = utils::format_hint_path(&self.path, *sstable_id);
            if hint_path.exists() {
                fs::remove_file(hint_path)?;
            }
        }

        let merge_sstable = SSTable::new(&merge_path, false)?;
        let merge_sstable_size = merge_sstable.size();

        self.sstables.insert(max_sstable_id, merge_sstable);

        if merge_hint_path.exists() {
            self.build_keydir_from_hint(&merge_hint_path)?;
        } else {
            self.build_keydir_from_sstable(max_sstable_id)?;
        }

        log::debug!(
            "keydir updated for compact and merge to: {}",
            max_sstable_id
        );

        Ok((max_sstable_id, merge_sstable_size))
    }
}
