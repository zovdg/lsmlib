//! SSTable Module.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::File;
use std::iter::Peekable;
use std::path::Path;

use crate::error::Result;

use super::format::{DiskEntry, EntryIO};
use super::logfile::LogFile;

#[derive(Debug)]
pub struct SSTable {
    inner: LogFile,
    reader: File,
}

impl AsRef<LogFile> for SSTable {
    fn as_ref(&self) -> &LogFile {
        &self.inner
    }
}

impl AsMut<LogFile> for SSTable {
    fn as_mut(&mut self) -> &mut LogFile {
        &mut self.inner
    }
}

impl SSTable {
    pub fn new(path: impl AsRef<Path>, writeable: bool) -> Result<Self> {
        let inner = LogFile::new(path, writeable)?;
        let reader = inner.reader()?;

        Ok(SSTable { inner, reader })
    }

    pub fn path(&self) -> &Path {
        self.inner.path.as_path()
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn size(&self) -> u64 {
        self.inner.size().unwrap()
    }

    pub fn truncate(&mut self, offset: u64) -> Result<()> {
        self.inner.truncate(offset)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.inner.sync()
    }

    /// Save key-value pair to segement file.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<DiskEntry> {
        self.write_entry(DiskEntry::new(key.to_vec(), value.to_vec()))
    }

    pub fn write_entry(&mut self, disk_entry: DiskEntry) -> Result<DiskEntry> {
        let path = self.inner.path.to_path_buf();

        let w = self.inner.writer()?;

        log::trace!(
            "append {} to segement file {}",
            String::from_utf8_lossy(&disk_entry.key),
            path.display()
        );

        let offset = disk_entry.write_to(w)?;

        log::trace!(
            "successfully append {} to data file {}",
            &disk_entry,
            path.display()
        );

        Ok(disk_entry.offset(offset).file_id(self.inner.id))
    }

    /// Read key value in data file.
    pub fn read(&mut self, offset: u64) -> Result<Option<DiskEntry>> {
        log::trace!(
            "read key value with offset {} in data file {}",
            offset,
            self.inner.path.display()
        );

        if self.inner.size()? < offset {
            return Ok(None);
        }

        match DiskEntry::read_from(&mut self.reader, offset)? {
            None => Ok(None),
            Some(entry) => {
                log::trace!(
                    "successfully read {} from data log file {}",
                    &entry,
                    self.inner.path.display()
                );

                Ok(Some(entry))
            }
        }
    }

    pub fn iter(&mut self) -> DiskEntryIter {
        DiskEntryIter {
            reader: self.inner.reader().unwrap(),
            offset: 0,
            file_id: self.inner.id,
        }
    }
}

pub struct DiskEntryIter {
    reader: File,
    offset: u64,
    file_id: u64,
}

impl Iterator for DiskEntryIter {
    type Item = DiskEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match DiskEntry::read_from(&mut self.reader, self.offset).unwrap() {
            None => None,
            Some(entry) => {
                let entry = entry.offset(self.offset).file_id(self.file_id);
                self.offset += entry.size();
                Some(entry)
            }
        }
    }
}

pub fn read_sstable(path: &Path) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut sst = SSTable::new(path, false)?;

    let mut items = BTreeMap::new();

    for entry in sst.iter() {
        let _ = items.insert(entry.key, entry.value);
    }

    Ok(items)
}

pub struct CompactMergeIter {
    sstables: Vec<RefCell<Peekable<DiskEntryIter>>>,
}

impl CompactMergeIter {
    pub fn new(iters: Vec<DiskEntryIter>) -> Self {
        let mut sstables = Vec::new();
        for iter in iters {
            sstables.push(RefCell::new(iter.peekable()));
        }

        Self { sstables }
    }
}

impl Iterator for CompactMergeIter {
    type Item = DiskEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut top: Option<(usize, Vec<u8>, u32)> = None;
        for (index, iter) in self.sstables.iter().enumerate() {
            if let Some(entry) = iter.borrow_mut().peek() {
                match &top {
                    None => top = Some((index, entry.key.clone(), entry.timestamp())),
                    Some((top_index, key, timestamp)) => {
                        if *key > entry.key {
                            top = Some((index, entry.key.clone(), entry.timestamp()));
                        } else if *key == entry.key {
                            if *timestamp < entry.timestamp() {
                                // next last iter.
                                self.sstables[*top_index].borrow_mut().next();
                                // use newer data.
                                top = Some((index, entry.key.clone(), entry.timestamp()));
                            } else {
                                // drop older data.
                                iter.borrow_mut().next();
                            }
                        }
                    }
                }
            }
        }

        match top {
            None => None,
            Some((index, _, _)) => self.sstables[index].borrow_mut().next(),
        }
    }
}
