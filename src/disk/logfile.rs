//! Log File Module.

use std::fs::{self, File};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{LSMLibError, Result};
use crate::utils;

#[derive(Debug)]
pub(crate) struct LogFile {
    /// file path.
    pub(crate) path: PathBuf,

    /// file id.
    pub(crate) id: u64,

    /// Mark current data file can be writable or not.
    writeable: bool,

    /// Current file writer.
    writer: Option<File>,
}

impl LogFile {
    pub(crate) fn new(path: impl AsRef<Path>, writeable: bool) -> Result<Self> {
        let path = path.as_ref();

        // Data name must starts with valid file id.
        let file_id = utils::parse_file_id(path).expect(&format!(
            "file id not found in file path: {}",
            path.display()
        ));

        let writer = if writeable {
            Some(
                fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(path)?,
            )
        } else {
            None
        };

        Ok(Self {
            path: path.to_path_buf(),
            id: file_id,
            writeable,
            writer,
        })
    }

    /// Truncate file.
    pub(crate) fn truncate(&mut self, offset: u64) -> Result<()> {
        let w = self.writer()?;

        w.seek(SeekFrom::Start(offset))?;
        w.set_len(offset)?;
        w.sync_all()?;

        Ok(())
    }

    pub(crate) fn reader(&self) -> Result<File> {
        Ok(fs::File::open(&self.path)?)
    }

    pub(crate) fn writer(&mut self) -> Result<&mut File> {
        self.writer
            .as_mut()
            .ok_or_else(|| LSMLibError::FileNotWriteable(self.path.to_path_buf()))
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.writer()?.sync_all()?;
        Ok(())
    }

    /// Datafile size current.
    pub(crate) fn size(&self) -> Result<u64> {
        let reader = self.reader()?;
        Ok(reader.metadata()?.len())
    }
}
