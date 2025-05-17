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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // use_which == 0 => using a
    // use_which == 1 => using b
    use_which: u8,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn decide_use_which_iter(&self) -> u8 {
        if !self.a.is_valid() {
            return 1;
        }
        if !self.b.is_valid() {
            return 0;
        }
        if self.a.key() < self.b.key() {
            return 0;
        }
        1
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, use_which: 0 };
        while iter.a.is_valid() && iter.b.is_valid() && iter.a.key() == iter.b.key() {
            iter.b.next()?;
        }
        iter.use_which = iter.decide_use_which_iter();
        Ok(iter)
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_which == 0 {
            return self.a.key();
        }
        self.b.key()
    }

    fn value(&self) -> &[u8] {
        if self.use_which == 0 {
            return self.a.value();
        }
        self.b.value()
    }

    fn is_valid(&self) -> bool {
        self.use_which == 0 && self.a.is_valid() || self.use_which == 1 && self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.use_which == 0 {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        while self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        self.use_which = self.decide_use_which_iter();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
