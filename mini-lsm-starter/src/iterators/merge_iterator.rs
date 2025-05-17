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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // case 1: iters is empty.
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        // case 2: iters not empty, but all invalid.
        let all_invalid = iters.iter().all(|tmp_iter| !tmp_iter.is_valid());
        if all_invalid {
            let mut mut_iters = iters;
            return Self {
                iters: BinaryHeap::new(),
                current: Some(HeapWrapper(0, mut_iters.pop().unwrap())),
            };
        }
        // case 3: iters not empty, some are valid.
        let mut heap = BinaryHeap::new();
        for (idx, tmp_iter) in iters.into_iter().enumerate() {
            if tmp_iter.is_valid() {
                heap.push(HeapWrapper(idx, tmp_iter));
            }
        }
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        if self.current.is_none() {
            return false;
        }
        self.current.as_ref().unwrap().1.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let mut_current = self.current.as_mut().unwrap();
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            if inner_iter.1.key() == mut_current.1.key() {
                if let e @ Err(_) = inner_iter.1.next() {
                    // fail to next, pop this wrong iter from the heap.
                    PeekMut::pop(inner_iter);
                    return e;
                }
                if !inner_iter.1.is_valid() {
                    // current peak iter is used up.
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }
        mut_current.1.next()?;
        if !mut_current.1.is_valid() {
            // If the current iterator is invalid, pop it from the heap and select the next one.
            let pop_res = self.iters.pop();
            if pop_res.is_some() {
                *mut_current = pop_res.unwrap();
            }
            return Ok(());
        }
        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *mut_current < *inner_iter {
                std::mem::swap(&mut *inner_iter, mut_current);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + 1
    }
}
