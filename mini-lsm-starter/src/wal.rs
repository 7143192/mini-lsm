// REMOVE THIS LINE after fully implementing this functionality
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

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).open(_path)?;
        let cloned_file = file.try_clone()?;
        let mut reader = BufReader::new(file);
        let mut data_buf = Vec::new();
        // read data into the target buffer.
        reader.read_to_end(&mut data_buf)?;
        // traverse the data buffer and reconstruct all manifest records.
        let mut buf_ptr = &data_buf[..];
        // week 2 day 7: add checksum logic for WAL.
        let mut kv_vec: Vec<u8> = Vec::new();
        let u32_size = std::mem::size_of::<u32>();
        while !buf_ptr.is_empty() {
            kv_vec.clear();
            // read key len.
            let key_len_bytes = &buf_ptr[..8];
            kv_vec.extend(key_len_bytes);
            let key_len = u64::from_be_bytes(key_len_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[8..];
            // read key data.
            let key_data = &buf_ptr[..key_len as usize];
            kv_vec.extend(key_data);
            buf_ptr = &buf_ptr[key_len as usize..];
            // read value len.
            let value_len_bytes = &buf_ptr[..8];
            kv_vec.extend(value_len_bytes);
            let value_len = u64::from_be_bytes(value_len_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[8..];
            // read value data.
            let value_data = &buf_ptr[..value_len as usize];
            kv_vec.extend(value_data);
            buf_ptr = &buf_ptr[value_len as usize..];
            let checksum_bytes = &buf_ptr[..u32_size];
            let checksum = u32::from_be_bytes(checksum_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[u32_size..];
            let recovered_checksum = crc32fast::hash(kv_vec.as_ref());
            if checksum != recovered_checksum {
                return Err(anyhow::anyhow!(
                    "WAL checksum mismatch: expected {}, got {}",
                    recovered_checksum,
                    checksum
                ));
            }
            _skiplist.insert(
                Bytes::copy_from_slice(key_data),
                Bytes::copy_from_slice(value_data),
            );
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(cloned_file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut kv_vec: Vec<u8> = Vec::new();
        // write key len.
        let key_len = _key.len();
        let key_len_bytes = key_len.to_be_bytes();
        file.write_all(&key_len_bytes)?;
        // write key data.
        file.write_all(_key)?;
        // write value len.
        let value_len = _value.len();
        let value_len_bytes = value_len.to_be_bytes();
        file.write_all(&value_len_bytes)?;
        // write value data.
        file.write_all(_value)?;
        // week 2 day 7: add checksum logic for WAL.
        kv_vec.extend(key_len_bytes);
        kv_vec.extend(_key);
        kv_vec.extend(value_len_bytes);
        kv_vec.extend(_value);
        let checksum = crc32fast::hash(kv_vec.as_ref());
        file.write_all(&checksum.to_be_bytes())?;
        // sync.
        file.flush()?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
