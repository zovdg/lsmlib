//! request handling

pub struct WriteRequest {}

pub struct ReadRequest {}

pub enum Request {
    Get(Vec<u8>),          // Read Request
    ListKeys,              // Read Request
    Contains(Vec<u8>),     // Read Request
    Put(Vec<u8>, Vec<u8>), // Write Request
    Remove(Vec<u8>),       // Write Request
}
