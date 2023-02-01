//! KeyDir Module.

use std::collections::HashMap;

use crate::disk::format::{DiskEntry, HintEntry};
use crate::error::{LSMLibError, Result};

/// keyDirEntry represents.
#[derive(Debug, Copy, Clone)]
pub struct KeydirEntry {
    /// file id the entry is associated.
    pub file_id: u64,

    /// offset of the entry in the file.
    pub offset: u64,

    /// size of the entry in bytes.
    pub size: u64,

    /// timestamp of the entry.
    pub timestamp: u32,
}

impl TryFrom<&DiskEntry> for KeydirEntry {
    type Error = LSMLibError;

    fn try_from(value: &DiskEntry) -> Result<Self> {
        let file_id = if let Some(file_id) = value.file_id {
            file_id
        } else {
            return Err(LSMLibError::Custom("file_id is None".to_string()));
        };

        let offset = if let Some(offset) = value.offset {
            offset
        } else {
            return Err(LSMLibError::Custom("offset is None".to_string()));
        };

        Ok(Self {
            file_id,
            offset,
            size: value.size(),
            timestamp: value.timestamp(),
        })
    }
}

impl TryFrom<&HintEntry> for KeydirEntry {
    type Error = LSMLibError;

    fn try_from(value: &HintEntry) -> Result<Self> {
        let file_id = if let Some(file_id) = value.file_id {
            file_id
        } else {
            return Err(LSMLibError::Custom("file_id is None".to_string()));
        };

        Ok(Self {
            file_id,
            offset: value.offset(),
            size: value.size(),
            timestamp: value.timestamp(),
        })
    }
}

/// Keydir methods.
pub trait Keydir: Default {
    /// Returns a reference to corresponding entry.
    fn get(&self, key: &[u8]) -> Option<&KeydirEntry>;

    /// Puts a key and entry into the keydir.
    fn put(&mut self, key: Vec<u8>, entry: KeydirEntry) -> &KeydirEntry;

    /// Removes a key and entry from the keydir.
    fn remove(&mut self, key: &[u8]);

    /// List all keys in the keydir.
    fn keys(&self) -> Vec<Vec<u8>>;

    /// Iterate all keys in datastore and call function `f`
    /// for each entry.
    ///
    /// If function `f` returns an `Err`, it stops iteration
    /// and propagates the `Err` to the caller.
    ///
    /// You can continue iteration manually by return `Ok(true)`,
    /// or stop iteration by returning `Ok(false)`.
    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &mut KeydirEntry) -> Result<bool>;

    /// length of the keys in the keydir
    fn len(&self) -> u64;

    /// Return `true` if datastore contains the given key.
    fn contains_key(&self, key: &[u8]) -> bool;

    /// Return on disk size.
    fn disk_size(&self) -> u64;
}

/// Keydir represented as a hashmap.
#[derive(Debug, Default)]
pub struct HashmapKeydir {
    mapping: HashMap<Vec<u8>, KeydirEntry>,
}

impl Keydir for HashmapKeydir {
    fn get(&self, key: &[u8]) -> Option<&KeydirEntry> {
        self.mapping.get(key)
    }

    fn put(&mut self, key: Vec<u8>, entry: KeydirEntry) -> &KeydirEntry {
        self.mapping
            .entry(key)
            .and_modify(|e| {
                if e.timestamp <= entry.timestamp {
                    *e = entry.clone();
                }
            })
            .or_insert(entry)
    }

    fn remove(&mut self, key: &[u8]) {
        self.mapping.remove(key);
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        self.mapping.keys().cloned().collect()
    }

    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &mut KeydirEntry) -> Result<bool>,
    {
        for (k, v) in self.mapping.iter_mut() {
            if f(k, v)? {
                break;
            }
        }

        Ok(())
    }

    fn len(&self) -> u64 {
        self.mapping.len() as u64
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.mapping.contains_key(key)
    }

    fn disk_size(&self) -> u64 {
        self.mapping.iter().map(|e| e.1.size).sum()
    }
}
