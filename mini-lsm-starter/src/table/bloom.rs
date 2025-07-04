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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let u32_size = std::mem::size_of::<u32>();
        let u8_size = std::mem::size_of::<u8>();
        let filter = &buf[..buf.len() - u8_size - u32_size];
        let k = buf[buf.len() - u32_size - u8_size];
        // week 2 day 7: add checksum logic.
        let checksum_bytes = &buf[buf.len() - u32_size..];
        let checksum = u32::from_be_bytes(checksum_bytes.try_into().unwrap());
        let expected_checksum = crc32fast::hash(buf[..buf.len() - u32_size].as_ref());
        if checksum != expected_checksum {
            return Err(anyhow::anyhow!(
                "Bloom Filter Checksum mismatch: expected {}, got {}",
                expected_checksum,
                checksum
            ));
        }
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut tmp_buf: Vec<u8> = Vec::new();
        tmp_buf.extend(&self.filter);
        tmp_buf.put_u8(self.k);
        buf.extend(tmp_buf.as_slice());
        // week 2 day 7: add checksum logic.
        let bloom_checksum = crc32fast::hash(&tmp_buf);
        buf.put_u32(bloom_checksum);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);
        // TODO: build the bloom filter
        for key in keys {
            let mut h = *key;
            let delta = h.rotate_left(15); // h is the key hash
            for _ in 0..k {
                let idx = (h as usize) % nbits;
                filter.set_bit(idx, true);
                h = h.wrapping_add(delta);
            }
        }
        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            // TODO: probe the bloom filter
            let delta = h.rotate_left(15);
            let mut mut_h = h;
            for i in 0..self.k {
                let idx = (mut_h as usize) % nbits;
                if !self.filter.get_bit(idx) {
                    return false;
                }
                mut_h = mut_h.wrapping_add(delta);
            }
            true
        }
    }
}
