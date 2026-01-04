use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::memtable::MemTable;
use crate::sstable::{SSTableBuilder, SSTableReader, SearchResult};
use crate::wal::{WAL, WALIterator};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompactionPolicy {
    Disabled,
    Periodic(Duration),
}

pub struct Database {
    pub path: PathBuf,
    memtable: MemTable,
    wal: WAL,
    sstables: Vec<SSTableReader>,
    compaction_policy: CompactionPolicy,
    last_compaction_time: SystemTime,
}

const MEMTABLE_THRESHOLD: usize = 4 * 1024 * 1024; // 4MB

impl Database {
    pub fn new(path: impl AsRef<Path>) -> Database {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            fs::create_dir_all(&path).expect("Unable to create database directory");
        }

        let wal_path = path.join("wal.log");
        let wal = WAL::new(&wal_path).expect("Unable to create WAL");

        Database {
            path,
            memtable: MemTable::new(),
            wal,
            sstables: Vec::new(),
            compaction_policy: CompactionPolicy::Disabled,
            last_compaction_time: SystemTime::now(),
        }
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Database> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Ok(Database::new(&path));
        }

        let wal_path = path.join("wal.log");
        let wal = WAL::new(&wal_path)?;
        let mut memtable = MemTable::new();

        if wal_path.exists() {
            let iter = WALIterator::new(&wal_path)?;
            for entry in iter {
                let (key, val) = entry?;
                if let Some(v) = val {
                    memtable.set(key, v);
                } else {
                    memtable.del(key);
                }
            }
        }

        let mut sstables = Vec::new();
        let entries = fs::read_dir(&path)?;
        let mut sstable_files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "sst"))
            .collect();

        sstable_files.sort();
        sstable_files.reverse();

        for sst_path in sstable_files {
            sstables.push(SSTableReader::new(sst_path)?);
        }

        Ok(Database {
            path,
            memtable,
            wal,
            sstables,
            compaction_policy: CompactionPolicy::Disabled,
            last_compaction_time: SystemTime::now(),
        })
    }

    pub fn set_compaction_policy(&mut self, policy: CompactionPolicy) {
        self.compaction_policy = policy;
    }

    fn try_trigger_compaction(&mut self) -> io::Result<()> {
        let duration = match self.compaction_policy {
            CompactionPolicy::Periodic(d) => d,
            CompactionPolicy::Disabled => return Ok(()),
        };

        if self
            .last_compaction_time
            .elapsed()
            .map_or(false, |e| e >= duration)
        {
            self.compact()?;
        }
        Ok(())
    }

    pub fn set(&mut self, key: String, value: String) {
        self.wal.set(&key, &value).expect("Failed to write to WAL");
        self.memtable.set(key, value);

        if self.memtable.size_bytes() >= MEMTABLE_THRESHOLD {
            self.flush_memtable().expect("Failed to flush memtable");
        }

        self.try_trigger_compaction()
            .expect("Auto-compaction failed");
    }

    pub fn batch_set(&mut self, entries: Vec<(String, String)>) {
        self.wal
            .batch_set(&entries)
            .expect("Failed to write to WAL");
        for (key, value) in entries {
            self.memtable.set(key, value);
        }

        if self.memtable.size_bytes() >= MEMTABLE_THRESHOLD {
            self.flush_memtable().expect("Failed to flush memtable");
        }

        self.try_trigger_compaction()
            .expect("Auto-compaction failed");
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(val_opt) = self.memtable.get(key) {
            return val_opt;
        }

        for sstable in &mut self.sstables {
            match sstable.get(key) {
                Ok(SearchResult::Found(val)) => return Some(val),
                Ok(SearchResult::Deleted) => return None,
                Ok(SearchResult::NotFound) => continue,
                Err(_) => continue, // Should handle error appropriately, but for now continue
            }
        }

        None
    }

    pub fn del(&mut self, key: &str) {
        self.wal.del(key).expect("Failed to write to WAL");
        self.memtable.del(key.to_string());

        if self.memtable.size_bytes() >= MEMTABLE_THRESHOLD {
            self.flush_memtable().expect("Failed to flush memtable");
        }

        self.try_trigger_compaction()
            .expect("Auto-compaction failed");
    }

    pub fn get_by_prefix(&mut self, prefix: &str) -> Vec<String> {
        let mut map = std::collections::BTreeMap::new();

        // 1. Scan SSTables (oldest to newest, so newer overwrites older)
        for sstable in self.sstables.iter_mut().rev() {
            let start = prefix;
            let end = format!("{}{}", prefix, '\u{10FFFF}'); // Max char

            if let Ok(entries) = sstable.scan(start, &end) {
                for (k, v) in entries {
                    map.insert(k, Some(v));
                }
            }
        }

        // 2. Scan MemTable
        for (k, v) in self.memtable.iter() {
            if k.starts_with(prefix) {
                map.insert(k.clone(), v.clone());
            }
        }

        // 3. Collect results (filter out tombstones)
        map.into_values().flatten().collect()
    }

    pub fn flush(&mut self) {
        self.flush_memtable().expect("Failed to flush");
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        if self.memtable.len() == 0 {
            return Ok(());
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let sst_name = format!("sstable_{}.sst", timestamp);
        let sst_path = self.path.join(sst_name);

        let mut builder = SSTableBuilder::new(&sst_path)?;

        for (key, val_opt) in self.memtable.iter() {
            if let Some(val) = val_opt {
                builder.add(key, val)?;
            } else {
                builder.delete(key)?;
            }
        }

        builder.finish()?;

        // Add to list (at the front, as it's newest)
        self.sstables.insert(0, SSTableReader::new(sst_path)?);

        // Clear MemTable and WAL
        self.memtable.clear();
        self.wal.clear()?;

        Ok(())
    }

    pub fn compact(&mut self) -> io::Result<()> {
        self.flush_memtable()?;

        if self.sstables.is_empty() {
            return Ok(());
        }

        let old_sstables = std::mem::take(&mut self.sstables);

        let mut iters: Vec<_> = old_sstables
            .into_iter()
            .map(|sst| sst.into_iter().peekable())
            .collect();

        // 3. Start new SSTable
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let new_sst_name = format!("sstable_compacted_{}.sst", timestamp);
        let new_sst_path = self.path.join(&new_sst_name);

        let mut builder = SSTableBuilder::new(&new_sst_path)?;

        // 5. Merge Loop
        loop {
            // Find the iterator with the smallest key
            let mut best_idx = None;
            let mut min_key: Option<&String> = None;

            for (i, iter) in iters.iter_mut().enumerate() {
                if let Some(Ok((key, _))) = iter.peek() {
                    if min_key.map_or(true, |mk| key < mk) {
                        min_key = Some(key);
                        best_idx = Some(i);
                    }
                }
            }

            if let Some(idx) = best_idx {
                // Consume the element from best_idx
                let (key, val) = iters[idx].next().unwrap()?;
                eprintln!("Selected from Iter {}: key={}, val={:?}", idx, key, val);

                if let Some(v) = val {
                    builder.add(&key, &v)?;
                }

                // Advance other iterators if they have the same key
                for (i, iter) in iters.iter_mut().enumerate() {
                    if i == idx {
                        continue;
                    }

                    if let Some(Ok((k, _))) = iter.peek() {
                        if k == &key {
                            iter.next(); // Discard shadowed version
                        }
                    }
                }
            } else {
                // No more elements
                break;
            }
        }

        builder.finish()?;

        // 6. Delete old files
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext != "sst" {
                    continue;
                }

                if path.file_name() != Some(std::ffi::OsStr::new(&new_sst_name)) {
                    fs::remove_file(path)?;
                }
            }
        }

        // 7. Update self.sstables
        self.sstables = vec![SSTableReader::new(new_sst_path)?];

        // Update timestamp
        self.last_compaction_time = SystemTime::now();

        Ok(())
    }
}
