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

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

fn check_sst_overlap(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    first_key: KeySlice,
    last_key: KeySlice,
) -> bool {
    match upper {
        Bound::Excluded(key) if key <= first_key.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < first_key.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match lower {
        Bound::Excluded(key) if key >= last_key.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > last_key.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn check_key_in_sst(key: &[u8], first_key: KeySlice, last_key: KeySlice) -> bool {
    if key < first_key.raw_ref() || key > last_key.raw_ref() {
        return false;
    }
    true
}

fn get_in_memtables(_key: &[u8], snapshot: Arc<LsmStorageState>) -> (Result<Option<Bytes>>, i32) {
    // check current muttable memtable first.
    let get_bytes = snapshot.memtable.get(_key);
    if get_bytes.is_some() {
        if let Some(bytes) = get_bytes {
            if !bytes.is_empty() {
                return (Ok(Some(bytes)), 1);
            }
            return (Ok(None), 1);
        }
    }
    // then check all previous immutable memtables.
    for imm_memtable in snapshot.imm_memtables.iter() {
        let imm_get_bytes = imm_memtable.get(_key);
        if imm_get_bytes.is_some() {
            if let Some(imm_bytes) = imm_get_bytes {
                if !imm_bytes.is_empty() {
                    return (Ok(Some(imm_bytes)), 2);
                }
                return (Ok(None), 2);
            }
        }
    }
    // not found in the end.
    (Ok(None), 3)
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // week 1 day 5: seek in memtables first, if found, return, if not found, seek inside level 0 sstables.
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let get_in_memtable_res = get_in_memtables(_key, snapshot.clone());
        if get_in_memtable_res.1 != 3 {
            return get_in_memtable_res.0;
        }
        // week 1 day 5: return None in memtables, try to seek in all level 0 sstables.
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_id).clone().unwrap();
            if !check_key_in_sst(
                _key,
                sst.first_key().as_key_slice(),
                sst.last_key().as_key_slice(),
            ) {
                continue;
            }
            let sst_iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(sst),
                KeySlice::from_slice(_key),
            )?;
            if sst_iter.is_valid() {
                if sst_iter.value().is_empty() {
                    return Ok(None);
                }
                let bytes = Bytes::copy_from_slice(sst_iter.value());
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let read_state = self.state.read();
        let res = read_state.memtable.put(_key, _value);
        let cur_size = read_state.memtable.approximate_size();
        if cur_size >= self.options.target_sst_size {
            drop(read_state);
            let state_lock = self.state_lock.lock();
            self.force_freeze_memtable(&state_lock)?;
        }
        res
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let read_state = self.state.read();
        let res = read_state.memtable.put(_key, b"");
        let cur_size = read_state.memtable.approximate_size();
        if cur_size >= self.options.target_sst_size {
            drop(read_state);
            let state_lock = self.state_lock.lock();
            self.force_freeze_memtable(&state_lock)?;
        }
        res
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // acquire write lock first.
        let mut write_state = self.state.write();
        // create a new memtable.
        let new_sst_id = self.next_sst_id();
        let new_memtable = Arc::new(MemTable::create(new_sst_id));
        // change current memtable to a immutable table and push to the front of the vector.
        let mut tmp_write_state = write_state.as_ref().clone();
        let old_memtable = tmp_write_state.memtable.clone();
        tmp_write_state.imm_memtables.insert(0, old_memtable);
        tmp_write_state.memtable = new_memtable;
        *write_state = Arc::new(tmp_write_state);
        drop(write_state);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // collect mem_table iterators first.
        let mut mem_iters = vec![];
        mem_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for imm_memtable in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(imm_memtable.scan(_lower, _upper)));
        }
        // create memTable MergeIterator.
        let memtable_merge_iter = MergeIterator::create(mem_iters);
        // generate and collect sstable iterators.
        let mut sst_iters = vec![];
        // week 1 day 5: only collect iters in level 0 for now.
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap();
            if !check_sst_overlap(
                _lower,
                _upper,
                sst.first_key().as_key_slice(),
                sst.last_key().as_key_slice(),
            ) {
                continue;
            }
            // only collect sst iter whhich range is overlapped with the target range.
            match _lower {
                Bound::Excluded(key) => {
                    let mut sst_iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sst),
                        KeySlice::from_slice(key),
                    )?;
                    while sst_iter.is_valid() && sst_iter.key().raw_ref() <= key {
                        let res = sst_iter.next();
                        if res.is_err() {
                            exit(0);
                        }
                    }
                    sst_iters.push(Box::new(sst_iter));
                }
                Bound::Included(key) => {
                    let sst_iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sst),
                        KeySlice::from_slice(key),
                    )?;
                    sst_iters.push(Box::new(sst_iter));
                }
                Bound::Unbounded => {
                    let sst_iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sst))?;
                    sst_iters.push(Box::new(sst_iter));
                }
            }
        }
        // create ssTable MergeIterator.
        let sstable_merge_iter = MergeIterator::create(sst_iters);
        // create and return result iter.
        Ok(FusedIterator::new(
            LsmIterator::new(
                TwoMergeIterator::create(memtable_merge_iter, sstable_merge_iter).unwrap(),
                _upper,
            )
            .unwrap(),
        ))
    }
}
