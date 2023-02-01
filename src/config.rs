//! Config and Default Constants Definitions Module.

pub(crate) const DATA_FILE_SUFFIX: &str = ".data";
pub(crate) const HINT_FILE_SUFFIX: &str = ".hint";
pub(crate) const WAL_FILE_SUFFIX: &str = ".wal";
pub(crate) const DEFAULT_MAX_LOG_LENGTH: u64 = 32 * 1024 * 1024; // 32MB
pub(crate) const DEFAULT_MAX_KEY_SIZE: u64 = 64;
pub(crate) const DEFAULT_MAX_VALUE_SIZE: u64 = 65536;

pub(crate) const SSTABLE_DIR: &str = "sstables";
pub(crate) const U64_SZ: usize = std::mem::size_of::<u64>();

#[derive(Debug, Copy, Clone)]
pub struct Config {
    /// If on-disk uncompressed sstable data exceeds in-memory usage
    /// by this proportion, a full-compaction of all sstables will occur.
    /// This is only likely to happen in situations where
    /// multiple versions of most of the database's keys exist
    /// in multiple sstables, but should never happen for workloads
    /// where mostly new keys are being written.
    pub max_space_amp: u8,

    /// When the log file exceeds this size, a new compressed and compacted
    /// sstable will be flushed to disk and the log file will be truncated.
    pub max_log_length: u64,

    pub max_key_size: u64,

    pub max_value_size: u64,

    /// When the background compactor thread looks for contiguous
    /// ranges of sstables to merge, it will require all sstables
    /// to be at least 1/`merge_ratio` * the size of the first sstable
    /// in the contiguous window under consideration.
    pub merge_ratio: u8,

    /// When the background compactor thread looks for ranges of
    /// sstables to merge, it will require ranges to be at least
    /// this long.
    pub merge_window: u8,

    /// All inserts go directly to a `BufWriter` wrapping the log
    /// file. This option determines how large that in-memory buffer is.
    pub log_bufwriter_size: u32,

    /// The level of compression to use for the sstables with zstd.
    pub zstd_sstable_compression_level: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_space_amp: 2,
            max_log_length: DEFAULT_MAX_LOG_LENGTH,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_value_size: DEFAULT_MAX_VALUE_SIZE,
            merge_ratio: 3,
            merge_window: 10,
            log_bufwriter_size: 32 * 1024,
            zstd_sstable_compression_level: 3,
        }
    }
}
