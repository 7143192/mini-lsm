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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut res = self.data.clone();
        for item in &self.offsets {
            res.put_u16(*item);
        }
        res.put_u16(self.offsets.len() as u16);
        res.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut res_offsets: Vec<u16> = Vec::new();
        let u16_size = std::mem::size_of::<u16>();
        let offsets_num = (&data[data.len() - u16_size..]).get_u16();
        let res_data = data[0..data.len() - u16_size * ((offsets_num + 1) as usize)].to_vec();
        let mut i: u16 = 0;
        while i < offsets_num {
            res_offsets.push(
                (&data[(data.len() - u16_size * ((offsets_num + 1) as usize))
                    + (i as usize) * u16_size..])
                    .get_u16(),
            );
            i += 1;
        }
        Self {
            data: res_data,
            offsets: res_offsets,
        }
    }
}
