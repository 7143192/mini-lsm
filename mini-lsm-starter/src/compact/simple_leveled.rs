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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut level_file_nums = Vec::new();
        level_file_nums.push(_snapshot.l0_sstables.len());
        for (_, sst_ids) in _snapshot.levels.iter() {
            level_file_nums.push(sst_ids.len());
        }
        let l0_file_nums = _snapshot.l0_sstables.len();
        if l0_file_nums >= self.options.level0_file_num_compaction_trigger {
            // case 1: level 0 contains too many files, trigger a compaction.
            println!("compaction triggered at level 0 because L0 has {} SSTs >= {}", l0_file_nums, self.options.level0_file_num_compaction_trigger);
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            })
        }
        for i in 0..self.options.max_levels {
            if i == 0 && _snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger {
                continue;
            }
            let lower_level = i + 1;
            let size_ratio = level_file_nums[lower_level] as f64 / level_file_nums[i] as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                let mut upper_level_sst_ids = Vec::new();
                if i == 0 {
                    upper_level_sst_ids.extend(_snapshot.l0_sstables.clone());
                } else {
                    upper_level_sst_ids.extend(_snapshot.levels[i - 1].1.clone());
                }
                println!("compaction triggered at level {} and {} with size ratio {}", i, lower_level, size_ratio);
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i),
                    upper_level_sst_ids,
                    lower_level,
                    lower_level_sst_ids: _snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        // l0 + l1 compaction and l_i + l_(i+1) (i > 0) compaction should be handled differently.
        match _task.upper_level {
            None => {
                // l0 + l1 compaction.
                let l0_sst_ids = snapshot.l0_sstables.clone();
                let l1_sst_ids = snapshot.levels[0].1.clone();
                snapshot.l0_sstables.clear();
                snapshot.levels[0].1 = _output.to_vec();
                files_to_remove.extend(l0_sst_ids);
                files_to_remove.extend(l1_sst_ids);
            },
            Some(level_id) => {
                // l_i + l_(i+1) (i > 0) compaction.
                let upper_level_sst_ids = snapshot.levels[level_id - 1].1.clone();
                let lower_level_sst_ids = snapshot.levels[_task.lower_level - 1].1.clone();
                snapshot.levels[level_id - 1].1.clear();
                snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();
                files_to_remove.extend(upper_level_sst_ids);
                files_to_remove.extend(lower_level_sst_ids);
            },
        }
        (snapshot, files_to_remove)

    }
}
