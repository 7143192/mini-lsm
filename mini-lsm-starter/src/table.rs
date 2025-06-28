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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        let meta_num = block_meta.len();
        let old_buf_len = buf.len();
        buf.put_u16(meta_num as u16);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        // week 2 day 7: add checksum logic for BlockMeta.
        let block_meta_checksum = crc32fast::hash(&buf[old_buf_len..]);
        buf.put_u32(block_meta_checksum);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        let u32_size = std::mem::size_of::<u32>();
        let buf_to_checksum = &buf[..buf.remaining() - u32_size];
        let num = buf.get_u16() as usize;
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        let checksum = buf.get_u32();
        // week 2 day 7: verify the checksum.
        let expected_checksum = crc32fast::hash(buf_to_checksum);
        if checksum != expected_checksum {
            panic!(
                "BlockMeta checksum mismatch: expected {}, got {}",
                expected_checksum, checksum
            );
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    // this first_key is first key of the ENTIRE SSTable, not a single block
    first_key: KeyBytes,
    // the last_key is the last key of the ENTIRE SSTable, not a single block
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_size = file.size();
        let u32_size = std::mem::size_of::<u32>();
        let bloom_offset_bytes = file.read(file_size - u32_size as u64, u32_size as u64)?;
        let bloom_offset = (&bloom_offset_bytes[..]).get_u32() as usize;
        let bloom_bytes = file.read(
            bloom_offset as u64,
            file_size - u32_size as u64 - bloom_offset as u64,
        )?;
        let bloom = Bloom::decode(&bloom_bytes[..])?;
        let meta_offset_bytes =
            file.read(bloom_offset as u64 - u32_size as u64, u32_size as u64)?;
        let meta_offset = (&meta_offset_bytes[..]).get_u32() as usize;
        let meta_bytes = file.read(
            meta_offset as u64,
            bloom_offset as u64 - u32_size as u64 - meta_offset as u64,
        )?;
        let meta = BlockMeta::decode_block_meta(&meta_bytes[..]);
        Ok(Self {
            file,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key: meta.first().unwrap().first_key.clone(),
            last_key: meta.last().unwrap().last_key.clone(),
            block_meta: meta,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_offset = self.block_meta[block_idx].offset;
        let mut end_offset = self.block_meta_offset;
        let u32_size = std::mem::size_of::<u32>();
        if block_idx != self.block_meta.len() - 1 {
            end_offset = self.block_meta[block_idx + 1].offset;
        }
        let target_len = (end_offset - start_offset) as u64;
        let block_data_bytes = self.file.read(start_offset as u64, target_len)?;
        let block_data = &block_data_bytes[..(target_len - u32_size as u64) as usize];
        let checksum_bytes = &block_data_bytes[(target_len - u32_size as u64) as usize..];
        let checksum = (&checksum_bytes[..]).get_u32();
        // week 2 day 7: verify the checksum.
        let expected_checksum = crc32fast::hash(block_data);
        if checksum != expected_checksum {
            return Err(anyhow::anyhow!(
                "Checksum mismatch: expected {}, got {}",
                expected_checksum,
                checksum
            ));
        }
        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let meta_num = self.block_meta.len();
        let mut res: usize = meta_num - 1;
        for i in 1..meta_num {
            let first_key = self.block_meta[i].first_key.clone();
            if first_key.as_key_slice() > key {
                // println!(
                //     "first key:{:?}, target key:{:?}",
                //     Bytes::copy_from_slice(first_key.raw_ref()),
                //     Bytes::copy_from_slice(key.raw_ref())
                // );
                res = i - 1;
                break;
            }
        }
        res
        // self.block_meta
        //     .partition_point(|meta| meta.first_key.as_key_slice() <= key)
        //     .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
