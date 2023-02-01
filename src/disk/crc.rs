//! CRC Module.

pub(super) fn hash(k: &[u8], v: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(k);
    hasher.update(v);

    // we XOR the hash to make sure it's something other than 0 when empty,
    // because 0 is an easy value to create accidentally or via corruption.
    hasher.finalize() ^ 0xFF
}

#[inline]
pub(super) fn hash_batch_len(len: usize) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&(len as u64).to_le_bytes());

    hasher.finalize() ^ 0xFF
}
