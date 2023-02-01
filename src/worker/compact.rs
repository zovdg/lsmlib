//! Compactor Module.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, RwLock};

use crate::config::Config;
use crate::disk::{
    format::HintEntry,
    hint::HintFile,
    sstable::{self, SSTable},
};
use crate::error::Result;
use crate::storage::{KeydirUpdate, Store};
use crate::utils;

pub enum CompactorMessage {
    NewSSTable { id: u64, size: u64 },
    Stop(mpsc::Sender<()>),
    HeartBeat(mpsc::Sender<()>),
}

pub struct Compactor {
    /// Dir of the Datastore.
    pub(crate) path: PathBuf,

    /// Map of sstables, which is file id and it's size.
    pub(crate) sstables: BTreeMap<u64, u64>,

    /// Disk Storage.
    pub(crate) store: Arc<RwLock<Store>>,

    /// Inbox of message.
    pub(crate) inbox: mpsc::Receiver<CompactorMessage>,

    /// config of the Datastore.
    pub(crate) config: Config,
}

impl Compactor {
    pub fn run(mut self) {
        while self.tick() {}
        log::info!("Compactor worker quitting...");
    }

    pub fn tick(&mut self) -> bool {
        match self.inbox.recv() {
            Ok(message) => {
                if !self.handle_message(message) {
                    return false;
                }
            }
            Err(e) => {
                log::error!("recv error: {:?}", e);
                return false;
            }
        }

        // only compact one run at a time before checking
        // for new messages.
        if let Err(e) = self.sstable_maintenance() {
            log::error!(
                "error while compacting sstables \
                in the background: {:?}",
                e
            );
        }

        return true;
    }

    fn handle_message(&mut self, msg: CompactorMessage) -> bool {
        match msg {
            CompactorMessage::NewSSTable { id, size } => {
                self.sstables.insert(id, size);
                true
            }
            CompactorMessage::Stop(dropper) => {
                drop(dropper);
                false
            }
            CompactorMessage::HeartBeat(dropper) => {
                drop(dropper);
                true
            }
        }
    }

    fn sstable_maintenance(&mut self) -> Result<()> {
        let on_disk_size: u64 = self.sstables.values().sum();

        log::debug!("disk size: {}", on_disk_size);
        if self.sstables.len() < self.config.merge_window.max(2) as usize {
            log::debug!("sstable files less 2, pass compacting...");
            return Ok(());
        }

        for window in self
            .sstables
            .iter()
            .collect::<Vec<_>>()
            .windows(self.config.merge_window.max(2) as usize)
        {
            if window
                .iter()
                .skip(1)
                .all(|w| *w.1 * self.config.merge_ratio as u64 > *window[0].1)
            {
                let run_to_compact: Vec<u64> = window.into_iter().map(|(id, _sum)| **id).collect();

                self.compact_sstable_run(&run_to_compact)?;
                return Ok(());
            }
        }

        Ok(())
    }

    // This function must be able to crash at any point without
    // leaving the system in an unrecoverable state, or without
    // losing data. This function must be nullpotent from the
    // external API surface's perspective.
    fn compact_sstable_run(&mut self, sstable_ids: &[u64]) -> Result<()> {
        log::debug!(
            "trying to compact sstable_ids: {:?}",
            sstable_ids
                .iter()
                .map(|id| utils::format_sstable_path(&self.path, *id))
                .collect::<Vec<_>>()
        );

        let max_sstable_id = sstable_ids
            .iter()
            .max()
            .copied()
            .expect("compact_sstable_run called with empty set of sst ids");

        let merge_tmp_path = utils::format_sstable_tmp_path(&self.path, max_sstable_id);
        if merge_tmp_path.exists() {
            log::debug!(
                "compact sstable_ids: {:?} already finished, waiting for keydir to be updated",
                sstable_ids
            );
            return Ok(());
        }

        let mut sstables = Vec::new();
        for sstable_id in sstable_ids.iter() {
            let path = utils::format_sstable_path(&self.path, *sstable_id);
            let mut sstable = SSTable::new(path, false)?;
            sstables.push(sstable.iter());
        }

        // let merge_tmp_path = utils::format_sstable_tmp_path(&self.path, max_sstable_id);
        let mut merge_sstable = SSTable::new(&merge_tmp_path, true)?;

        let merge_hint_tmp_path = utils::format_hint_tmp_path(&self.path, max_sstable_id);
        let mut merge_hint = HintFile::new(&merge_hint_tmp_path, true)?;

        let ms_iter = sstable::CompactMergeIter::new(sstables);
        for entry in ms_iter {
            // write to merge sstable.
            let disk_entry = merge_sstable.write_entry(entry)?;

            // write hint file.
            merge_hint.write_entry(HintEntry::from(&disk_entry))?;
        }

        // sync all write.
        merge_sstable.sync()?;

        log::debug!("compacting file generated...");

        // to updating keydir.
        let (sstable_id, size) = self.store.write().unwrap().compact_and_merge(sstable_ids)?;

        self.sstables.insert(sstable_id, size);

        for sstable_id in sstable_ids {
            if max_sstable_id == *sstable_id {
                continue;
            }

            self.sstables
                .remove(sstable_id)
                .expect("compacted sstable not persent in sstables");
        }

        log::debug!("compacting finished...");

        Ok(())
    }
}
