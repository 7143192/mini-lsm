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

use std::fs::OpenOptions;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)?,
            )),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut res_vec: Vec<ManifestRecord> = Vec::new();
        let file = OpenOptions::new().read(true).append(true).open(_path)?;
        let cloned_file = file.try_clone()?;
        let mut reader = BufReader::new(file);
        let mut data_buf = Vec::new();
        // read data into the target buffer.
        reader.read_to_end(&mut data_buf)?;
        // traverse the data buffer and reconstruct all manifest records.
        let mut buf_ptr = &data_buf[..];
        // week 2 day 7: add checksum logic for manifest.
        let mut checksum_vec: Vec<u8> = Vec::new();
        while !buf_ptr.is_empty() {
            checksum_vec.clear();
            // read length
            let len_bytes = &buf_ptr[..8];
            checksum_vec.extend(len_bytes);
            let len = u64::from_be_bytes(len_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[8..];
            // read data
            let data_slice = &buf_ptr[..len as usize];
            checksum_vec.extend(data_slice);
            let record: ManifestRecord = serde_json::from_slice::<ManifestRecord>(data_slice)?;
            buf_ptr = &buf_ptr[len as usize..];
            let checksum_bytes = &buf_ptr[..4];
            let checksum = u32::from_be_bytes(checksum_bytes.try_into().unwrap());
            buf_ptr = &buf_ptr[4..];
            let recovered_checksum = crc32fast::hash(checksum_vec.as_ref());
            if checksum != recovered_checksum {
                return Err(anyhow::anyhow!(
                    "Manifest checksum mismatch: expected {}, got {}",
                    recovered_checksum,
                    checksum
                ));
            }
            res_vec.push(record);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(cloned_file)),
            },
            res_vec,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        // acquire file write lock first.
        let mut file = self.file.lock();
        // encode.
        let mut checksum_vec: Vec<u8> = Vec::new();
        let json_vec = serde_json::to_vec(&_record).unwrap();
        // write serialized data length to manifest file.
        file.write_all(&(json_vec.len() as u64).to_be_bytes())?;
        checksum_vec.extend_from_slice(&(json_vec.len() as u64).to_be_bytes());
        // write real record data to manifest file.
        file.write_all(&json_vec)?;
        checksum_vec.extend_from_slice(&json_vec);
        // week 2 day 7: add checksum logic for manifest.
        let checksum = crc32fast::hash(checksum_vec.as_ref());
        file.write_all(&checksum.to_be_bytes())?;
        // fsync to disk.
        file.sync_all()?;
        Ok(())
    }
}
