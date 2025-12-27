use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub struct Database {
    data: HashMap<String, String>,
    path: PathBuf,
    file: File,
}

impl Database {
    pub fn new(path: impl AsRef<Path>) -> Database {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("Unable to create database file");

        Database {
            data: HashMap::new(),
            path,
            file,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Database> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let mut data = HashMap::new();
        let reader = BufReader::new(&file);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() { continue; }
            
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            if parts.is_empty() { continue; }

            match parts[0] {
                "SET" => {
                    if parts.len() == 3 {
                        let key: String = serde_json::from_str(parts[1])
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        let value: String = serde_json::from_str(parts[2])
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        data.insert(key, value);
                    }
                }
                "DEL" => {
                    if parts.len() == 2 {
                        let key: String = serde_json::from_str(parts[1])
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        data.remove(&key);
                    }
                }
                _ => {}
            }
        }

        Ok(Database {
            data,
            path,
            file,
        })
    }

    pub fn set(&mut self, key: String, value: String) {
        let key_json = serde_json::to_string(&key).expect("Failed to serialize key");
        let value_json = serde_json::to_string(&value).expect("Failed to serialize value");
        
        writeln!(self.file, "SET {} {}", key_json, value_json).expect("Failed to write to log");
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        match self.data.get(key) {
            Some(value) => Some(value),
            None => panic!("Key {key} not found in database."),
        }
    }

    pub fn del(&mut self, key: &str) {
        let key_json = serde_json::to_string(&key).expect("Failed to serialize key");
        writeln!(self.file, "DEL {}", key_json).expect("Failed to write to log");
        
        match self.data.remove(key) {
            Some(_) => (),
            None => panic!("Key {key} not found in database."),
        }
    }

    pub fn flush(&mut self) {
        let tmp_path = self.path.with_extension("tmp");
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .expect("Unable to create temp file for compaction");

        for (k, v) in &self.data {
            let key_json = serde_json::to_string(k).expect("Failed to serialize key");
            let value_json = serde_json::to_string(v).expect("Failed to serialize value");
            writeln!(tmp_file, "SET {} {}", key_json, value_json).expect("Failed to write to temp file");
        }

        fs::rename(&tmp_path, &self.path).expect("Failed to replace database file");
        
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .expect("Unable to reopen database file");
    }
}