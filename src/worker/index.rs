//! Indexor Module.

use std::sync::mpsc;

pub enum IndexorMessage {
    NewSSTableUpdate(u64),
    CompactUpdate(Vec<u64>),
    Stop(mpsc::Sender<()>),
    HeartBeat(mpsc::Sender<()>),
}

pub struct Indexor {}
