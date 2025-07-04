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
use bytes::Bytes;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let first_block = table.read_block(0).unwrap();
        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(first_block),
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let first_block = self.table.read_block(0).unwrap();
        self.blk_iter = BlockIterator::create_and_seek_to_first(first_block);
        self.blk_idx = 0;
        Ok(())
    }

    fn seek_to_key_helper(table: &Arc<SsTable>, key: KeySlice) -> (usize, BlockIterator) {
        let mut block_idx = table.find_block_idx(key);
        let block = table.read_block(block_idx).unwrap();
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !blk_iter.is_valid() {
            println!(
                "block {} is invalid after seeking for key {:?}",
                block_idx,
                Bytes::copy_from_slice(key.raw_ref())
            );
            block_idx += 1;
            if block_idx == table.block_meta.len() {
                return (block_idx, blk_iter);
            }
            let new_block = table.read_block(block_idx).unwrap();
            blk_iter = BlockIterator::create_and_seek_to_first(new_block);
        }
        (block_idx, blk_iter)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (idx, iter) = SsTableIterator::seek_to_key_helper(&table, key);
        Ok(Self {
            table,
            blk_iter: iter,
            blk_idx: idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (idx, iter) = SsTableIterator::seek_to_key_helper(&self.table, key);
        self.blk_idx = idx;
        self.blk_iter = iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx == self.table.block_meta.len() {
                return Ok(());
            }
            let block_ptr = self.table.read_block(self.blk_idx).unwrap();
            self.blk_iter = BlockIterator::create_and_seek_to_first(block_ptr);
        }
        Ok(())
    }
}
