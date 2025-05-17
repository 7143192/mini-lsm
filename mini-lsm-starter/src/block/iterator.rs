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

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_block_first_key(&self) -> KeyVec {
        let mut data = &self.data[..];
        let first_common_length = data.get_u16();
        let first_key_length = data.get_u16();
        let u16_size = std::mem::size_of::<u16>();
        let first_key_bytes =
            &self.data[2 * u16_size..(2 * u16_size + (first_key_length as usize))];
        KeyVec::from_vec(first_key_bytes.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: block.get_block_first_key(),
            block,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut res = BlockIterator::new(block);
        res.seek_to_first();
        res
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut res = BlockIterator::new(block);
        res.seek_to_key(key);
        res
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        if self.key.is_empty() {
            return false;
        }
        true
    }

    pub fn seek_to_target_idx(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }
        let start_offset = self.block.offsets[index] as usize;
        let mut target_data = &self.block.data[start_offset..];
        let common_key_len = target_data.get_u16() as usize;
        let target_key_len = target_data.get_u16() as usize;
        let u16_size = std::mem::size_of::<u16>();
        let key_start = start_offset + u16_size * 2;
        let key_end = key_start + target_key_len;
        let key_bytes = &self.block.data[key_start..key_end];
        self.idx = index;
        if common_key_len == 0 {
            self.key = KeyVec::from_vec(key_bytes.to_vec());
        } else {
            let mut common_key = self.first_key.raw_ref()[..common_key_len].to_vec();
            common_key.extend_from_slice(key_bytes);
            self.key = KeyVec::from_vec(common_key);
        }
        let mut val_start = key_end;
        let mut target_val_data = &self.block.data[val_start..];
        val_start += u16_size;
        let target_val_len = target_val_data.get_u16() as usize;
        let val_end = val_start + target_val_len;
        self.value_range = (val_start, val_end);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_target_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() - 1 {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }
        self.idx += 1;
        self.seek_to_target_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let offsets_len = self.block.offsets.len();
        let mut i = 0_usize;
        while i <= offsets_len {
            self.seek_to_target_idx(i);
            if self.key.as_key_slice() >= key {
                break;
            }
            i += 1;
        }
    }
}
