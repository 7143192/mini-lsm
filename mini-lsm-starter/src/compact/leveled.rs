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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let snapshot = _snapshot.clone();
        let mut overlapped_sst_ids: Vec<usize> = Vec::new();
        let target_min_key = _sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().first_key())
            .min()
            .cloned()
            .unwrap();
        let target_max_key = _sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().last_key())
            .max()
            .cloned()
            .unwrap();
        let level_sst_ids = snapshot.levels[_in_level - 1].1.clone();
        for sst_id in level_sst_ids.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap().clone();
            if sst.first_key().clone() > target_max_key || sst.last_key().clone() < target_min_key {
                continue;
            }
            overlapped_sst_ids.push(*sst_id);
        }
        overlapped_sst_ids
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        // initialize target_level_size and real_level_size first.
        let mut target_level_size: Vec<usize> = Vec::with_capacity(self.options.max_levels);
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            target_level_size.push(0_usize);
            real_level_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size() as usize)
                    .sum::<usize>(),
            );
        }
        // update target_level_size according to real_level_size.
        if real_level_size[self.options.max_levels - 1] > base_level_size_bytes {
            target_level_size[self.options.max_levels - 1] =
                real_level_size[self.options.max_levels - 1];
        } else {
            target_level_size[self.options.max_levels - 1] = base_level_size_bytes;
        }
        for i in 0..(self.options.max_levels - 1) {
            let next_level_size = target_level_size[self.options.max_levels - 1 - i];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[self.options.max_levels - 2 - i] = this_level_size;
            }
        }
        // then update the base_level.
        for i in 0..(self.options.max_levels - 1) {
            if target_level_size[self.options.max_levels - 2 - i] > 0 {
                base_level = self.options.max_levels - i - 1;
            }
        }
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // case 1: compact l0 with base_level when there are too many ssts in l0.
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }
        // case 2: trigger a compaction according to priority_ratios of levels.
        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let prio = real_level_size[level] as f64 / target_level_size[level] as f64;
            priorities.push(prio);
        }
        let mut updated = false;
        let mut max_ratio = 0.0;
        let mut level = 0;
        for (i, &ratio) in priorities.iter().enumerate() {
            if ratio > max_ratio {
                max_ratio = ratio;
                level = i + 1; // levels are 1-indexed
                updated = true;
            }
        }
        if updated && max_ratio > 1.0 {
            // let level = *level;
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove: Vec<usize> = Vec::new();
        files_to_remove.extend(_task.upper_level_sst_ids.clone());
        files_to_remove.extend(_task.lower_level_sst_ids.clone());
        let upper_level_hash_set = _task
            .upper_level_sst_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let lower_level_hash_set = _task
            .lower_level_sst_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        // update upper level states first.
        match _task.upper_level {
            None => {
                let mut new_l0_sst_ids: Vec<usize> = Vec::new();
                let old_l0_sst_ids = snapshot.l0_sstables.clone();
                for sst_id in old_l0_sst_ids.iter() {
                    if upper_level_hash_set.contains(sst_id) {
                        continue;
                    }
                    new_l0_sst_ids.push(*sst_id);
                }
                snapshot.l0_sstables = new_l0_sst_ids;
            }
            Some(upper_level) => {
                let mut new_upper_level_sst_ids: Vec<usize> = Vec::new();
                let old_upper_level_sst_ids = snapshot.levels[upper_level - 1].1.clone();
                for sst_id in old_upper_level_sst_ids.iter() {
                    if upper_level_hash_set.contains(sst_id) {
                        continue;
                    }
                    new_upper_level_sst_ids.push(*sst_id);
                }
                snapshot.levels[upper_level - 1].1 = new_upper_level_sst_ids;
            }
        }
        // then update lower level states.
        let old_lower_level_sst_ids = snapshot.levels[_task.lower_level - 1].1.clone();
        let mut new_lower_level_sst_ids: Vec<usize> = Vec::new();
        for sst_id in old_lower_level_sst_ids.iter() {
            if lower_level_hash_set.contains(sst_id) {
                continue;
            }
            new_lower_level_sst_ids.push(*sst_id);
        }
        new_lower_level_sst_ids.extend(_output);
        // then sort all new lower level states according to their first keys.
        new_lower_level_sst_ids
            .sort_by_key(|sst_id| snapshot.sstables.get(sst_id).unwrap().first_key().clone());
        snapshot.levels[_task.lower_level - 1].1 = new_lower_level_sst_ids;
        (snapshot, files_to_remove)
    }
}
