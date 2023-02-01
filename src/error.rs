//! lib error definitions.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, LSMLibError>;

#[derive(Debug, Error)]
pub enum LSMLibError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    IntParse(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Glob(#[from] glob::GlobError),

    #[error(transparent)]
    Pattern(#[from] glob::PatternError),

    #[error("key '{}' not found", String::from_utf8_lossy(.0))]
    KeyNotFound(Vec<u8>),

    #[error("key is too large")]
    KeyIsTooLarge,

    #[error("value is too large")]
    ValueIsTooLarge,

    #[error("file '{}' is not writeable", .0.display())]
    FileNotWriteable(std::path::PathBuf),

    #[error("db is already locked")]
    AlreadyLocked,

    #[error("{}", .0)]
    Custom(String),
}
