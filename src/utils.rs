//! utils Module.

use std::path::{Path, PathBuf};

use crate::config;

pub(crate) fn parse_file_id(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .split('.')
        .next()?
        .parse::<u64>()
        .ok()
}

pub(crate) fn format_sstable_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:012}{}", id, config::DATA_FILE_SUFFIX))
}

pub(crate) fn format_hint_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:012}{}", id, config::HINT_FILE_SUFFIX))
}

pub(crate) fn format_sstable_tmp_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:012}{}-tmp", id, config::DATA_FILE_SUFFIX))
}

pub(crate) fn format_hint_tmp_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:012}{}-tmp", id, config::HINT_FILE_SUFFIX))
}

pub(crate) fn format_wal_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:012}{}", id, config::WAL_FILE_SUFFIX))
}
