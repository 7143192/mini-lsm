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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            // case 1: when tier number is too small, no need to perform compaction.
            return None;
        }
        let last_level_num = _snapshot.levels.last().unwrap().1.len();
        let all_upper_levels_num = _snapshot
            .levels
            .iter()
            .take(_snapshot.levels.len() - 1)
            .map(|(_, vec)| vec.len())
            .sum::<usize>();
        let space_amplification_ratio = all_upper_levels_num as f64 / last_level_num as f64;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64 / 100.0 {
            // case 2: space amplification is too large, need to perform a compaction.
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        let mut prev_sum = _snapshot.levels[0].1.len();
        let size_ratio = (self.options.size_ratio as f64 + 100.0) / 100.0;
        for i in 1.._snapshot.levels.len() {
            let cur_level_num = _snapshot.levels[i].1.len();
            if cur_level_num as f64 / prev_sum as f64 >= size_ratio {
                if i >= self.options.min_merge_width {
                    // case 3: size ratio is too large and levels to compact is enough, need to perform a compaction.
                    return Some(TieredCompactionTask {
                        tiers: _snapshot.levels[..i].to_vec(),
                        bottom_tier_included: false,
                    });
                }
            }
            prev_sum += cur_level_num;
        }
        // case 4: force a compaction if the level number is too large.
        if self.options.max_merge_width.is_none() || _snapshot.levels.len() >= self.options.max_merge_width.unwrap() {
        return Some(TieredCompactionTask {
            tiers: _snapshot.levels.clone(),
            bottom_tier_included: true,
        });
    }
        Some(TieredCompactionTask {
            tiers: _snapshot.levels.iter().take(self.options.max_merge_width.unwrap()).cloned().collect::<Vec<_>>(),
            bottom_tier_included: false,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove: Vec<usize> = Vec::new();
        let mut new_levels: Vec<(usize, Vec<usize>)> = Vec::new();
        let mut tiers_to_remove = _task.tiers.iter().map(|(id, vec)| (*id, vec.clone())).collect::<HashMap<_, _>>();
        let mut new_level_added = false;
        for (level_id, sst_ids) in _snapshot.levels.iter() {
            if let Some(level_files_to_remove) = tiers_to_remove.remove(level_id) {
                files_to_remove.extend(level_files_to_remove);
            } else {
                new_levels.push((*level_id, sst_ids.clone()));
            }
            if tiers_to_remove.is_empty() {
                if !new_level_added {
                    new_level_added = true;
                    new_levels.push((_output[0], _output.to_vec()));
                }
            }
        }
        snapshot.levels = new_levels;
        (snapshot, files_to_remove)
    }
}
