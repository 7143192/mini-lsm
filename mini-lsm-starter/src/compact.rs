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
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
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
    fn full_compact(
        &self,
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res_vec = Vec::new();
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
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
        /* start of version using concat iterator */
        let l0_merge_iter = MergeIterator::create(all_iters);
        let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;
        let mut merge_iter = TwoMergeIterator::create(l0_merge_iter, l1_concat_iter)?;
        /* end of version using concat iterator */
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while merge_iter.is_valid() {
            // skip deleted key here.
            if merge_iter.value().is_empty() {
                merge_iter.next()?;
                continue;
            }
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
        Ok(res_vec)
    }

    fn simple_task_compact(
        &self,
        upper_level_ids: Vec<usize>,
        lower_level_ids: Vec<usize>,
        is_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res_vec: Vec<Arc<SsTable>> = Vec::new();
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        let upper_level_ssts: Vec<Arc<SsTable>> = upper_level_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().clone())
            .collect();
        let lower_level_ssts: Vec<Arc<SsTable>> = lower_level_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().clone())
            .collect();
        let mut upper_iters = Vec::new();
        upper_level_ssts.iter().for_each(|sst| {
            upper_iters.push(Box::new(
                SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
            ));
        });
        let upper_merge_iter = MergeIterator::create(upper_iters);
        let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(lower_level_ssts)?;
        let mut two_level_merge_iter =
            TwoMergeIterator::create(upper_merge_iter, lower_concat_iter)?;
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while two_level_merge_iter.is_valid() {
            // skip deleted key here.
            if two_level_merge_iter.value().is_empty() && is_bottom_level {
                two_level_merge_iter.next()?;
                continue;
            }
            sst_builder.add(two_level_merge_iter.key(), two_level_merge_iter.value());
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
            two_level_merge_iter.next()?;
        }
        // add the last sstable to res_vec.
        // NOTE: must skip empty sst builder here!!!
        if !sst_builder.empty() {
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
        Ok(res_vec)
    }

    fn tiered_task_compact(
        &self,
        tiers: Vec<(usize, Vec<usize>)>,
        bottom_level_included: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        let mut res_vec: Vec<Arc<SsTable>> = Vec::new();
        let mut all_concat_iters = Vec::new();
        for (_, sst_ids) in tiers.iter() {
            let sstables: Vec<Arc<SsTable>> = sst_ids
                .iter()
                .map(|id| snapshot.sstables.get(id).unwrap().clone())
                .collect();
            all_concat_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                sstables,
            )?));
        }
        let mut merge_iter = MergeIterator::create(all_concat_iters);
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while merge_iter.is_valid() {
            // skip deleted key here.
            if merge_iter.value().is_empty() && bottom_level_included {
                merge_iter.next()?;
                continue;
            }
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
        Ok(res_vec)
    }

    fn leveled_task_compact(
        &self,
        upper_level_ids: Vec<usize>,
        lower_level_ids: Vec<usize>,
        is_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        self.simple_task_compact(upper_level_ids, lower_level_ids, is_bottom_level)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        // let mut res_vec: Vec<Arc<SsTable>> = Vec::new();
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let result = self.full_compact(l0_sstables.clone(), l1_sstables.clone())?;
                Ok(result.clone())
            }
            CompactionTask::Simple(simple_task) => {
                let result = self.simple_task_compact(
                    simple_task.upper_level_sst_ids.clone(),
                    simple_task.lower_level_sst_ids.clone(),
                    simple_task.is_lower_level_bottom_level,
                )?;
                Ok(result.clone())
            }
            CompactionTask::Tiered(tiered_task) => {
                let result = self.tiered_task_compact(
                    tiered_task.tiers.clone(),
                    tiered_task.bottom_tier_included,
                )?;
                Ok(result.clone())
            }
            CompactionTask::Leveled(leveled_task) => {
                let result = self.leveled_task_compact(
                    leveled_task.upper_level_sst_ids.clone(),
                    leveled_task.lower_level_sst_ids.clone(),
                    leveled_task.is_lower_level_bottom_level,
                )?;
                Ok(result.clone())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        // collect all l0 and l1 sstables to generate compactin task.
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels.first().unwrap().1.clone();
        let l0_sstables_clone = l0_sstables.clone();
        let l1_sstables_clone = l1_sstables.clone();
        let full_compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };
        // then call compact func to perform the real compaction.
        let compacted_sstables = self.compact(&full_compaction_task)?;
        {
            let state_lock = self.state_lock.lock();
            let new_l1_sst_ids: Vec<usize> =
                compacted_sstables.iter().map(|s| s.sst_id()).collect();
            let cloned_new_sst_ids = new_l1_sst_ids.clone();
            // then update storage state.
            let mut write_state = self.state.write();
            let mut tmp_write_state = write_state.as_ref().clone();
            // update indexes.
            tmp_write_state.l0_sstables.clear();
            tmp_write_state.levels[0].1 = new_l1_sst_ids;
            // update sstables map.
            l0_sstables_clone.iter().for_each(|id| {
                tmp_write_state.sstables.remove(id);
            });
            l1_sstables_clone.iter().for_each(|id| {
                tmp_write_state.sstables.remove(id);
            });
            compacted_sstables.iter().for_each(|sst| {
                tmp_write_state.sstables.insert(sst.sst_id(), sst.clone());
            });
            // update state.
            *write_state = Arc::new(tmp_write_state);
            drop(write_state);
            // add new manifest record here to record for force-full-compactions.
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(full_compaction_task, cloned_new_sst_ids.clone()),
            )?;
        }
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
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // try to generate a compaction task first.
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        let compaction_task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        if compaction_task.is_none() {
            return Ok(());
        }
        // // then perform the compaction according to the compaction task.
        // println!("before compaction:");
        // self.dump_structure();
        let compaction_task = compaction_task.unwrap();
        let compacted_sstables = self.compact(&compaction_task)?;
        let output: Vec<usize> = compacted_sstables.iter().map(|sst| sst.sst_id()).collect();
        let files_to_remove: Vec<usize> = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut ssts_to_remove: Vec<usize> = Vec::new();
            let mut new_sst_ids: Vec<usize> = Vec::new();
            for sst in compacted_sstables {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
                new_sst_ids.push(sst.sst_id());
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &compaction_task, &output, false);
            for file_to_remove in files_to_remove.iter() {
                snapshot.sstables.remove(file_to_remove);
                ssts_to_remove.push(*file_to_remove);
            }
            let mut write_state = self.state.write();
            *write_state = Arc::new(snapshot);
            drop(write_state);
            println!(
                "files to delete after a compaction:{:?}, files to add:{:?}",
                ssts_to_remove,
                output.clone()
            );
            // add logic to write new manifest record here to record for compaction.
            self.sync_dir()?; // sync dir files first.
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(compaction_task, new_sst_ids.clone()),
            )?;
            ssts_to_remove
        };
        for file_to_remove in files_to_remove.iter() {
            let path = self.path_of_sst(*file_to_remove);
            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("remove sstable failed: {}", e);
            }
        }
        // println!("after compaction:");
        // self.dump_structure();
        Ok(())
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
