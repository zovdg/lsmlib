//! Format: Entries Module.

use std::{
    fmt::Display,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::disk::crc::hash;
use crate::error::Result;

/// EntryIO trait.
pub trait EntryIO {
    type Entry;

    fn read_from<R>(r: &mut R, offset: u64) -> Result<Option<Self::Entry>>
    where
        R: Read + Seek;

    fn write_to<W>(&self, w: &mut W) -> Result<u64>
    where
        W: Write + Seek;
}

pub const HEADER_SIZE: usize = 16;

/// Entry Header
///
/// # fields:
/// - crc: u32
/// - timestamp: u32
/// - key_sz: u32
/// - value_sz: u32
///
#[derive(Debug, Clone)]
pub struct Header([u8; HEADER_SIZE]);

impl Header {
    pub fn new(crc: u32, timestamp: u32, key_sz: u32, value_sz: u32) -> Self {
        let mut buf = [0u8; HEADER_SIZE];

        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf[4..8].copy_from_slice(&timestamp.to_le_bytes());
        buf[8..12].copy_from_slice(&key_sz.to_le_bytes());
        buf[12..16].copy_from_slice(&value_sz.to_le_bytes());

        Self(buf)
    }

    pub fn crc(&self) -> u32 {
        u32::from_le_bytes(self.0[0..4].try_into().unwrap())
    }

    pub fn timestamp(&self) -> u32 {
        u32::from_le_bytes(self.0[4..8].try_into().unwrap())
    }

    pub fn key_sz(&self) -> u32 {
        u32::from_le_bytes(self.0[8..12].try_into().unwrap())
    }

    pub fn value_sz(&self) -> u32 {
        u32::from_le_bytes(self.0[12..16].try_into().unwrap())
    }
}

impl AsRef<[u8]> for Header {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; HEADER_SIZE]> for Header {
    fn from(buf: [u8; HEADER_SIZE]) -> Self {
        Self(buf)
    }
}

/// Disk Entry
#[derive(Debug, Clone)]
pub struct DiskEntry {
    /// header of the disk entry.
    header: Header,

    /// key of the disk entry.
    pub key: Vec<u8>,

    /// value of the disk entry.
    pub value: Vec<u8>,

    /// offset of the disk entry in the disk file.
    pub offset: Option<u64>,

    /// file id of the disk entry may stored.
    pub file_id: Option<u64>,
}

impl DiskEntry {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let crc = hash(&key, &value);
        let timestamp = chrono::Utc::now().timestamp().try_into().unwrap();
        let key_sz = key.len() as u32;
        let value_sz = value.len() as u32;
        let header = Header::new(crc, timestamp, key_sz, value_sz);

        Self {
            header,
            key,
            value,
            offset: None,
            file_id: None,
        }
    }

    pub fn crc(&self) -> u32 {
        self.header.crc()
    }

    pub fn timestamp(&self) -> u32 {
        self.header.timestamp()
    }

    pub fn size(&self) -> u64 {
        (HEADER_SIZE + self.key.len() + self.value.len()) as u64
    }

    pub fn entry_size(k: &[u8], v: &[u8]) -> u64 {
        (HEADER_SIZE + k.len() + v.len()) as u64
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn file_id(mut self, file_id: u64) -> Self {
        self.file_id = Some(file_id);
        self
    }

    pub fn is_validate(&self) -> bool {
        self.header.crc() == hash(&self.key, &self.value)
    }

    pub fn crc_expected(&self) -> u32 {
        self.header.crc()
    }

    pub fn crc_actual(&self) -> u32 {
        hash(&self.key, &self.value)
    }
}

impl Display for DiskEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DiskEntry(file_id={:?}, key='{}', offset={:?}, size={})",
            self.file_id,
            String::from_utf8_lossy(self.key.as_ref()),
            self.offset,
            self.size(),
        )
    }
}

impl EntryIO for DiskEntry {
    type Entry = Self;

    fn read_from<R>(r: &mut R, offset: u64) -> Result<Option<Self::Entry>>
    where
        R: Read + Seek,
    {
        r.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; HEADER_SIZE];
        if r.read(&mut buf)? == 0 {
            return Ok(None);
        }

        let header = Header::from(buf);

        let mut key = vec![0u8; header.key_sz() as usize];
        r.read_exact(&mut key)?;

        let mut value = vec![0u8; header.value_sz() as usize];
        r.read_exact(&mut value)?;

        Ok(Some(Self {
            header,
            key,
            value,
            offset: None,
            file_id: None,
        }))
    }

    fn write_to<W>(&self, w: &mut W) -> Result<u64>
    where
        W: Write + Seek,
    {
        let offset = w.stream_position()?;

        w.write_all(self.header.as_ref())?;
        w.write_all(self.key.as_ref())?;
        w.write_all(self.value.as_ref())?;

        Ok(offset)
    }
}

pub const HINT_HEADER_SIZE: usize = 20;

/// Hint Entry Header Structure.
///
/// # fields:
/// - offset: u64
/// - key_sz: u32
/// - value_sz: u32
/// - timestamp: u32
///
#[derive(Debug)]
pub struct HintHeader([u8; HINT_HEADER_SIZE]);

impl HintHeader {
    pub fn new(offset: u64, key_sz: u32, value_sz: u32, timestamp: u32) -> Self {
        let mut buf = [0u8; HINT_HEADER_SIZE];

        buf[0..8].copy_from_slice(&offset.to_le_bytes());
        buf[8..12].copy_from_slice(&key_sz.to_le_bytes());
        buf[12..16].copy_from_slice(&value_sz.to_le_bytes());
        buf[16..20].copy_from_slice(&timestamp.to_le_bytes());

        Self(buf)
    }

    pub fn offset(&self) -> u64 {
        u64::from_le_bytes(self.0[0..8].try_into().unwrap())
    }

    pub fn key_sz(&self) -> usize {
        u32::from_le_bytes(self.0[8..12].try_into().unwrap()) as usize
    }

    pub fn value_sz(&self) -> usize {
        u32::from_le_bytes(self.0[12..16].try_into().unwrap()) as usize
    }

    pub fn timestamp(&self) -> u32 {
        u32::from_le_bytes(self.0[16..20].try_into().unwrap())
    }
}

impl AsRef<[u8; HINT_HEADER_SIZE]> for HintHeader {
    fn as_ref(&self) -> &[u8; HINT_HEADER_SIZE] {
        &self.0
    }
}

impl From<[u8; HINT_HEADER_SIZE]> for HintHeader {
    fn from(buf: [u8; HINT_HEADER_SIZE]) -> Self {
        Self(buf)
    }
}

/// Entry in the hint file.
#[derive(Debug)]
pub struct HintEntry {
    /// header of hint entry.
    header: HintHeader,

    /// key of disk entry.
    pub key: Vec<u8>,

    /// file_id of hint entry, also is disk entry.
    pub file_id: Option<u64>,
}

impl HintEntry {
    pub fn new(key: Vec<u8>, offset: u64, size: u64, timestamp: u32) -> Self {
        let key_sz = key.len() as u32;
        let value_sz = size as u32 - HEADER_SIZE as u32 - key_sz;
        let header = HintHeader::new(offset, key_sz, value_sz, timestamp);
        Self {
            header,
            key,
            file_id: None,
        }
    }

    pub fn offset(&self) -> u64 {
        self.header.offset()
    }

    pub fn size(&self) -> u64 {
        (HEADER_SIZE + self.header.key_sz() + self.header.value_sz()) as u64
    }

    pub fn timestamp(&self) -> u32 {
        self.header.timestamp()
    }

    pub fn hint_size(&self) -> u64 {
        HINT_HEADER_SIZE as u64 + self.key.len() as u64
    }

    pub fn file_id(mut self, file_id: u64) -> Self {
        self.file_id = Some(file_id);
        self
    }

    pub fn key_sz(&self) -> usize {
        self.header.key_sz()
    }

    pub fn value_sz(&self) -> usize {
        self.header.value_sz()
    }
}

impl Display for HintEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HintEntry(key='{}', offset={}, size={})",
            String::from_utf8_lossy(self.key.as_ref()),
            self.offset(),
            self.size(),
        )
    }
}

impl From<&DiskEntry> for HintEntry {
    fn from(v: &DiskEntry) -> Self {
        let header = HintHeader::new(
            v.offset.unwrap(),
            v.key.len() as u32,
            v.value.len() as u32,
            v.timestamp(),
        );
        Self {
            header,
            key: v.key.clone(),
            file_id: v.file_id.clone(),
        }
    }
}

impl EntryIO for HintEntry {
    type Entry = Self;

    fn read_from<R>(r: &mut R, offset: u64) -> Result<Option<Self::Entry>>
    where
        R: Read + Seek,
    {
        r.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; HINT_HEADER_SIZE];
        if r.read(&mut buf)? == 0 {
            return Ok(None);
        }

        let header = HintHeader::from(buf);

        let mut key = vec![0u8; header.key_sz()];
        r.read_exact(&mut key)?;

        Ok(Some(Self::Entry {
            header,
            key,
            file_id: None,
        }))
    }

    fn write_to<W>(&self, w: &mut W) -> Result<u64>
    where
        W: Write + Seek,
    {
        let offset = w.stream_position()?;

        w.write_all(self.header.as_ref())?;
        w.write_all(self.key.as_ref())?;

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn test_disk_entry_io() {
        let entry = DiskEntry::new(b"hello".to_vec(), b"world".to_vec());

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        let offset = entry.write_to(&mut cursor).unwrap();
        assert_eq!(offset, 0);

        let entry1 = DiskEntry::read_from(&mut cursor, offset).unwrap();
        assert_eq!(entry1.is_some(), true);

        let e = entry1.unwrap();
        assert_eq!(e.key, b"hello".to_vec());
    }

    #[test]
    fn test_crc_check() {
        let mut entry = DiskEntry::new(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(entry.is_validate(), true);

        entry.value = b"hello".to_vec();
        assert_eq!(entry.is_validate(), false);
    }

    #[test]
    fn test_hint_entry_io() {
        let entry = HintEntry::new(b"hello".to_vec(), 0, 100, 0);

        assert_eq!(entry.header.key_sz(), 5);
        assert_eq!(entry.header.value_sz(), 100 - 5 - HEADER_SIZE);
        assert_eq!(entry.size(), 100);
        assert_eq!(entry.hint_size(), 5 + HINT_HEADER_SIZE as u64);

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        let offset = entry.write_to(&mut cursor).unwrap();
        assert_eq!(offset, 0);

        let entry1 = HintEntry::read_from(&mut cursor, offset).unwrap();
        assert_eq!(entry1.is_some(), true);

        let e = entry1.unwrap();
        assert_eq!(e.key, b"hello".to_vec());
        assert_eq!(e.size(), 100);
        assert_eq!(entry.hint_size(), 5 + HINT_HEADER_SIZE as u64);
    }
}
