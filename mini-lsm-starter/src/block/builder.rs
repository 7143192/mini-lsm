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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn cur_size(&self) -> usize {
        let offset_bytes: usize = 2;
        self.offsets.len() * offset_bytes + self.data.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    // #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // let offset_bytes: usize = 2;
        // if !self.is_empty()
        //     && self.cur_size() + key.len() + value.len() + 3 * offset_bytes > self.block_size
        // {
        //     return false;
        // }
        // // add off into vec.
        // self.offsets.push(self.data.len() as u16);
        // // add data bytes into vec.
        // self.data.put_u16(key.len() as u16);
        // self.data.put(key.raw_ref());
        // self.data.put_u16(value.len() as u16);
        // self.data.put(value);
        // if self.first_key.is_empty() {
        //     self.first_key = key.to_key_vec();
        // }
        // true
        self.add_with_prefix(key, value)
    }

    // week 1 day 7
    pub fn add_with_prefix(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let u16_size = std::mem::size_of::<u16>();
        // compute common len between target key and the first key first.
        let mut common_len = 0;
        if !self.first_key.is_empty() {
            for i in 0..std::cmp::min(self.first_key.len(), key.len()) {
                if self.first_key.raw_ref()[i] != key.raw_ref()[i] {
                    break;
                }
                common_len += 1;
            }
        }
        // compute rest len of he target key.
        let rest_len = key.len() - common_len;
        // check whether the block is full.
        if !self.is_empty()
            && self.cur_size() + rest_len + value.len() + 4 * u16_size > self.block_size
        {
            return false;
        }
        // add off into vec.
        self.offsets.push(self.data.len() as u16);
        // store an extra field to record common length with first key for a target key.
        self.data.put_u16(common_len as u16);
        // add data bytes into vec.
        self.data.put_u16(rest_len as u16);
        self.data.put(&key.raw_ref()[common_len..]);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
