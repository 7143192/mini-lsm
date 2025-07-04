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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use bytes::{BufMut, Bytes};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    pub fn empty(&self) -> bool {
        self.data.is_empty() && self.builder.is_empty()
    }

    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // set first key first if the first key is empty.
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().raw_ref().to_vec();
        }
        // then try to add this kv pair into current block.
        // week 1 day 7
        // let add_result = self.builder.add(key, value);
        let add_result = self.builder.add_with_prefix(key, value);
        if add_result {
            // if success, set last key and return directly.
            self.last_key = key.to_key_vec().raw_ref().to_vec();
            self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
            return;
        }
        // if current block is full, finish the old block and create a new block.
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone())),
        });
        let encoded_old_block = old_builder.build().encode();
        self.data.extend(encoded_old_block.clone());
        // week 2 day 7: add a checksum for a completed data block.
        let checksum = crc32fast::hash(&encoded_old_block.clone());
        self.data.put_u32(checksum);
        // insert this kv pair into the new block.
        // week 1 day 7
        // let new_block_add_result = self.builder.add(key, value);
        let new_block_add_result = self.builder.add_with_prefix(key, value);
        if !new_block_add_result {
            panic!("Failed to add kv pair to new block.");
        }
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
        self.first_key = key.to_key_vec().raw_ref().to_vec();
        self.last_key = key.to_key_vec().raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // force the current block to finish.
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone())),
        });
        let encoded_old_block = old_builder.build().encode();
        self.data.extend(encoded_old_block.clone());
        // week 2 day 7: add a checksum for a completed data block.
        let checksum = crc32fast::hash(&encoded_old_block.clone());
        self.data.put_u32(checksum);
        let meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(meta_offset as u32);
        let bloom_offset = self.data.len();
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(self.key_hashes.as_slice(), bits_per_key);
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_offset as u32);
        let file = FileObject::create(path.as_ref(), self.data)?;
        Ok(SsTable {
            id,
            file,
            block_meta_offset: meta_offset,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
