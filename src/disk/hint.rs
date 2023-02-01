//! Hint File Module.

use crate::error::Result;
use std::fs::File;
use std::path::Path;

use super::format::{EntryIO, HintEntry};
use super::logfile::LogFile;

pub struct HintFile {
    inner: LogFile,
}

impl AsRef<LogFile> for HintFile {
    fn as_ref(&self) -> &LogFile {
        &self.inner
    }
}

impl AsMut<LogFile> for HintFile {
    fn as_mut(&mut self) -> &mut LogFile {
        &mut self.inner
    }
}

impl HintFile {
    pub fn new(path: impl AsRef<Path>, writeable: bool) -> Result<Self> {
        let inner = LogFile::new(path, writeable)?;
        Ok(Self { inner })
    }

    pub fn path(&self) -> &Path {
        self.inner.path.as_path()
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn sync(&mut self) -> Result<()> {
        self.inner.sync()
    }

    pub fn write(
        &mut self,
        key: impl AsRef<[u8]>,
        offset: u64,
        size: u64,
        timestamp: u32,
    ) -> Result<u64> {
        self.write_entry(HintEntry::new(
            key.as_ref().to_vec(),
            offset,
            size,
            timestamp,
        ))
    }

    pub fn write_entry(&mut self, entry: HintEntry) -> Result<u64> {
        log::trace!("append {} to file {}", &entry, self.inner.path.display());
        let w = self.inner.writer().expect("hint file is not writeable");
        let offset = entry.write_to(w)?;
        // self.entries_written += 1;
        Ok(offset)
    }

    pub fn iter(&mut self) -> HintEntryIter {
        HintEntryIter {
            reader: self.inner.reader().unwrap(),
            offset: 0,
            file_id: self.inner.id,
        }
    }
}

pub struct HintEntryIter {
    reader: File,
    offset: u64,
    file_id: u64,
}

impl Iterator for HintEntryIter {
    type Item = HintEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match HintEntry::read_from(&mut self.reader, self.offset).unwrap() {
            None => None,
            Some(entry) => {
                self.offset += entry.hint_size();
                Some(entry.file_id(self.file_id))
            }
        }
    }
}
