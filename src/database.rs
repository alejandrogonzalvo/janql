use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;

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

        loop {
            let mut op_buf = [0u8; 1];
            match reader.read_exact(&mut op_buf) {
                Ok(_) => {},
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            
            let op = op_buf[0];
            // 1 byte for op
            let mut current_cmd_len = 1;

            match op {
                OP_SET => {
                    let key = BinaryCodec::read_string(&mut reader)?;
                    current_cmd_len += 4 + key.len() as u64;
                    
                    let val_len = BinaryCodec::read_u32(&mut reader)? as u64;
                    let val_pos = pos + current_cmd_len + 4; // +4 for val_len bytes
                    
                    // Skip value bytes
                    reader.seek_relative(val_len as i64)?;
                    
                    current_cmd_len += 4 + val_len;
                    
                    data.insert(key, CommandPos { pos: val_pos, len: val_len });
                }
                OP_DEL => {
                    let key = BinaryCodec::read_string(&mut reader)?;
                    current_cmd_len += 4 + key.len() as u64;
                    data.remove(&key);
                }
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid opcode")),
            }
            
            pos += current_cmd_len;
        }

        Ok(Database {
            data,
            path,
            file,
        })
    }

    pub fn set(&mut self, key: String, value: String) {
        self.file.seek(SeekFrom::End(0)).expect("Failed to seek");
        
        // Opcode
        self.file.write_all(&[OP_SET]).expect("Failed to write opcode");
        
        // Key
        BinaryCodec::write_string(&mut self.file, &key).expect("Failed to write key");
        
        // Value
        let val_len = value.len() as u32;
        self.file.write_all(&val_len.to_le_bytes()).expect("Failed to write val len");
        
        let val_pos = self.file.stream_position().expect("Failed to get val pos");
        self.file.write_all(value.as_bytes()).expect("Failed to write value");
        
        self.data.insert(key, CommandPos { pos: val_pos, len: val_len as u64 });
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        let cmd = self.data.get(key)?;
        
        self.file.seek(SeekFrom::Start(cmd.pos)).expect("Failed to seek");
        let mut buf = vec![0; cmd.len as usize];
        self.file.read_exact(&mut buf).expect("Failed to read value");
        
        String::from_utf8(buf).ok()
    }

    pub fn del(&mut self, key: &str) {
        self.file.seek(SeekFrom::End(0)).expect("Failed to seek");
        
        self.file.write_all(&[OP_DEL]).expect("Failed to write opcode");
        BinaryCodec::write_string(&mut self.file, key).expect("Failed to write key");
        
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
                
                let value = String::from_utf8(buf).expect("Invalid UTF8");
                
                // Write to temp file
                tmp_file.write_all(&[OP_SET]).expect("Failed to write opcode");
                BinaryCodec::write_string(&mut tmp_file, &key).expect("Failed to write key");
                
                let val_len = value.len() as u32;
                tmp_file.write_all(&val_len.to_le_bytes()).expect("Failed to write val len");
                
                let val_pos = current_pos + 1 + 4 + key.len() as u64 + 4;
                tmp_file.write_all(value.as_bytes()).expect("Failed to write value");
                
                let key_len = key.len() as u64;
                new_data.insert(key, CommandPos { pos: val_pos, len: val_len as u64 });
                
                current_pos += 1 + 4 + key_len + 4 + val_len as u64;
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

struct BinaryCodec;

impl BinaryCodec {
    fn write_string(writer: &mut impl Write, s: &str) -> io::Result<()> {
        let len = s.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(s.as_bytes())?;
        Ok(())
    }

    fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_string(reader: &mut impl Read) -> io::Result<String> {
        let len = Self::read_u32(reader)?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}