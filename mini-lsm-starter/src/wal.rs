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
        while !buf_ptr.is_empty() {
            // read key len.
            let key_len_bytes = &buf_ptr[..8];
            let key_len = u64::from_be_bytes(key_len_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[8..];
            // read key data.
            let key_data = &buf_ptr[..key_len as usize];
            buf_ptr = &buf_ptr[key_len as usize..];
            // read value len.
            let value_len_bytes = &buf_ptr[..8];
            let value_len = u64::from_be_bytes(value_len_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[8..];
            // read value data.
            let value_data = &buf_ptr[..value_len as usize];
            buf_ptr = &buf_ptr[value_len as usize..];
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
        // write key len.
        let key_len = _key.len();
        let key_len_bytes = key_len.to_be_bytes();
        self.file.lock().write_all(&key_len_bytes)?;
        // write key data.
        self.file.lock().write_all(_key)?;
        // write value len.
        let value_len = _value.len();
        let value_len_bytes = value_len.to_be_bytes();
        self.file.lock().write_all(&value_len_bytes)?;
        // write value data.
        self.file.lock().write_all(_value)?;
        // sync.
        self.file.lock().flush()?;
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
