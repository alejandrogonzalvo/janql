use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
struct CommandPos {
    pos: u64,
    len: u64,
}

pub struct Database {
    data: HashMap<String, CommandPos>,
    pub path: PathBuf,
    file: File,
}

impl Database {
    pub fn new(path: impl AsRef<Path>) -> Database {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true) 
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
        let mut reader = BufReader::new(&file);
        let mut pos = 0;
        let mut line = String::new();

        while reader.read_line(&mut line)? > 0 {
            let line_len = line.len() as u64;
            // Trim newline for parsing, but keep line_len for offset tracking
            let trimmed_line = line.trim_end();
            
            if trimmed_line.is_empty() { 
                pos += line_len;
                line.clear();
                continue; 
            }
            
            let parts: Vec<&str> = trimmed_line.splitn(3, ' ').collect();
            if parts.is_empty() { 
                pos += line_len;
                line.clear();
                continue; 
            }

            match parts[0] {
                "SET" => {
                    if parts.len() == 3 {
                        let key_part = parts[1];
                        let value_part = parts[2];
                        
                        match serde_json::from_str::<String>(key_part) {
                            Ok(key) => {
                                // Calculate value offset
                                // We need the offset of value_part within the UNTRIMMED line
                                // Assuming standard formatting "SET key value\n"
                                if let Some(val_offset_in_line) = line.rfind(value_part) {
                                    let value_pos = pos + val_offset_in_line as u64;
                                    let value_len = value_part.len() as u64;
                                    
                                    data.insert(key, CommandPos { pos: value_pos, len: value_len });
                                }
                            },
                            Err(_) => {} // Skip malformed keys
                        }
                    }
                }
                "DEL" => {
                    if parts.len() == 2 {
                        match serde_json::from_str::<String>(parts[1]) {
                            Ok(key) => {
                                data.remove(&key);
                            },
                            Err(_) => {}
                        }
                    }
                }
                _ => {}
            }
            
            pos += line_len;
            line.clear();
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
        
        let start_pos = self.file.seek(SeekFrom::End(0)).expect("Failed to seek");
        
        let line = format!("SET {} {}\n", key_json, value_json);
        self.file.write_all(line.as_bytes()).expect("Failed to write to log");
        
        let value_offset = start_pos + 4 + key_json.len() as u64 + 1;
        let value_len = value_json.len() as u64;

        self.data.insert(key, CommandPos { pos: value_offset, len: value_len });
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        let cmd = self.data.get(key)?;
        
        self.file.seek(SeekFrom::Start(cmd.pos)).expect("Failed to seek");
        let mut buf = vec![0; cmd.len as usize];
        self.file.read_exact(&mut buf).expect("Failed to read value");
        
        match serde_json::from_slice(&buf) {
            Ok(val) => Some(val),
            Err(_) => None, // Should not happen if log is consistent
        }
    }

    pub fn del(&mut self, key: &str) {
        let key_json = serde_json::to_string(&key).expect("Failed to serialize key");
        
        self.file.seek(SeekFrom::End(0)).expect("Failed to seek");
        writeln!(self.file, "DEL {}", key_json).expect("Failed to write to log");
        
        self.data.remove(key);
    }

    pub fn flush(&mut self) {
        let tmp_path = self.path.with_extension("tmp");
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .expect("Unable to create temp file for compaction");

        let keys: Vec<String> = self.data.keys().cloned().collect();
        let mut new_data = HashMap::new();
        let mut current_pos = 0;

        for key in keys {
            if let Some(cmd) = self.data.get(&key) {
                self.file.seek(SeekFrom::Start(cmd.pos)).expect("Failed to seek");
                let mut buf = vec![0; cmd.len as usize];
                self.file.read_exact(&mut buf).expect("Failed to read");
                
                let key_json = serde_json::to_string(&key).expect("Failed to serialize key");
                let value_json = String::from_utf8(buf).expect("Invalid UTF8");
                
                let line = format!("SET {} {}\n", key_json, value_json);
                tmp_file.write_all(line.as_bytes()).expect("Failed to write to temp file");
                
                let value_offset = current_pos + 4 + key_json.len() as u64 + 1;
                let value_len = value_json.len() as u64;
                
                new_data.insert(key, CommandPos { pos: value_offset, len: value_len });
                
                current_pos += line.len() as u64;
            }
        }

        fs::rename(&tmp_path, &self.path).expect("Failed to replace database file");
        
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .expect("Unable to reopen database file");
            
        self.data = new_data;
    }
}