// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        let mut res_vec: Vec<Arc<SsTable>> = Vec::new();
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_ssts: Vec<Arc<SsTable>> = l0_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();
                let l1_ssts: Vec<Arc<SsTable>> = l1_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();
                let mut all_iters = Vec::new();
                l0_ssts.iter().for_each(|sst| {
                    all_iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });
                l1_ssts.iter().for_each(|sst| {
                    all_iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });
                let mut merge_iter = MergeIterator::create(all_iters);
                let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                while merge_iter.is_valid() {
                    // skip deleted key here.
                    if merge_iter.value().is_empty() {
                        merge_iter.next()?;
                        continue;
                    }
                    println!(
                        "key: {:?}, value: {:?}",
                        std::str::from_utf8(merge_iter.key().raw_ref()).unwrap(),
                        std::str::from_utf8(merge_iter.value()).unwrap()
                    );
                    sst_builder.add(merge_iter.key(), merge_iter.value());
                    if sst_builder.estimated_size() > self.options.target_sst_size {
                        // if add is failed, generate a new sstable.
                        let new_sst_id = self.next_sst_id();
                        let new_sst = Arc::new(
                            sst_builder
                                .build(
                                    new_sst_id,
                                    Some(self.block_cache.clone()),
                                    self.path_of_sst(new_sst_id),
                                )
                                .unwrap(),
                        );
                        res_vec.push(new_sst);
                        sst_builder = SsTableBuilder::new(self.options.block_size);
                    }
                    merge_iter.next()?;
                }
                // add the last sstable to res_vec.
                let new_sst_id = self.next_sst_id();
                let new_sst = Arc::new(
                    sst_builder
                        .build(
                            new_sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(new_sst_id),
                        )
                        .unwrap(),
                );
                res_vec.push(new_sst);
            }
            _ => unreachable!(),
        }
        Ok(res_vec)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        // collect all l0 and l1 sstables to generate compactin task.
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels.get(0).unwrap().1.clone();
        let l0_sstables_clone = l0_sstables.clone();
        let l1_sstables_clone = l1_sstables.clone();
        let full_compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables,
            l1_sstables: l1_sstables,
        };
        // then call compact func to perform the real compaction.
        let compacted_sstables = self.compact(&full_compaction_task)?;
        // then update storage state.
        let mut write_state = self.state.write();
        let mut tmp_write_state = write_state.as_ref().clone();
        // update indexes.
        tmp_write_state.l0_sstables.clear();
        let new_l1_sst_ids = compacted_sstables.iter().map(|s| s.sst_id()).collect();
        tmp_write_state.levels[0].1 = new_l1_sst_ids;
        // update sstables map.
        l0_sstables_clone.iter().for_each(|id| {
            tmp_write_state.sstables.remove(id);
        });
        l1_sstables_clone.iter().for_each(|id| {
            tmp_write_state.sstables.remove(id);
        });
        // remove compacted sstables from disk.
        l0_sstables_clone.iter().for_each(|id| {
            let path = self.path_of_sst(*id);
            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("remove sstable failed: {}", e);
            }
        });
        l1_sstables_clone.iter().for_each(|id| {
            let path = self.path_of_sst(*id);
            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("remove sstable failed: {}", e);
            }
        });
        compacted_sstables.iter().for_each(|sst| {
            tmp_write_state.sstables.insert(sst.sst_id(), sst.clone());
        });
        // update state.
        *write_state = Arc::new(tmp_write_state);
        drop(write_state);
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        if snapshot.imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
