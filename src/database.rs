use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::memtable::MemTable;
use crate::sstable::{SSTableBuilder, SSTableReader};
use crate::wal::{WAL, WALIterator};

pub struct Database {
    pub path: PathBuf,
    memtable: MemTable,
    wal: WAL,
    sstables: Vec<SSTableReader>,
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
        })
    }

    pub fn set(&mut self, key: String, value: String) {
        self.wal.set(&key, &value).expect("Failed to write to WAL");
        self.memtable.set(key, value);

        if self.memtable.size_bytes() >= MEMTABLE_THRESHOLD {
            self.flush_memtable().expect("Failed to flush memtable");
        }
    }

    pub fn batch_set(&mut self, entries: Vec<(String, String)>) {
        self.wal.batch_set(&entries).expect("Failed to write to WAL");
        for (key, value) in entries {
            self.memtable.set(key, value);
        }

        if self.memtable.size_bytes() >= MEMTABLE_THRESHOLD {
            self.flush_memtable().expect("Failed to flush memtable");
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(val_opt) = self.memtable.get(key) {
            return val_opt;
        }

        for sstable in &mut self.sstables {
            if let Ok(Some(val)) = sstable.get(key) {
                return Some(val);
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
}