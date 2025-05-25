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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(SstConcatIterator {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        Ok(SstConcatIterator {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(SstConcatIterator {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let mut sst_iter: SsTableIterator =
            SsTableIterator::create_and_seek_to_key(sstables[0].clone(), key)?;
        let mut next_idx = 0;
        let sstables_clone = sstables.clone();
        for sst in sstables_clone {
            next_idx += 1;
            sst_iter = SsTableIterator::create_and_seek_to_key(sst, key)?;
            if sst_iter.is_valid() {
                break;
            }
        }
        Ok(SstConcatIterator {
            current: Some(sst_iter),
            next_sst_idx: next_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(SsTableIterator::key)
            .unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(SsTableIterator::value)
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(SsTableIterator::is_valid)
            .unwrap_or_default()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(current) = self.current.as_mut() {
            current.next()?;
        }

        if !self.is_valid() {
            let idx = self.next_sst_idx;
            self.next_sst_idx += 1;

            if let Some(sst) = self.sstables.get(idx) {
                self.current = Some(SsTableIterator::create_and_seek_to_first(sst.clone())?);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
