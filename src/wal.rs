use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

pub struct WAL {
    file: File,
    path: std::path::PathBuf,
}

const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;

impl WAL {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path.as_ref())?;

        Ok(Self {
            file,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.file.write_all(&[OP_SET])?;
        
        let key_len = key.len() as u32;
        self.file.write_all(&key_len.to_le_bytes())?;
        self.file.write_all(key.as_bytes())?;

        let val_len = value.len() as u32;
        self.file.write_all(&val_len.to_le_bytes())?;
        self.file.write_all(value.as_bytes())?;
        
        self.file.sync_data()?; // Ensure durability
        Ok(())
    }

    pub fn del(&mut self, key: &str) -> io::Result<()> {
        self.file.write_all(&[OP_DEL])?;
        
        let key_len = key.len() as u32;
        self.file.write_all(&key_len.to_le_bytes())?;
        self.file.write_all(key.as_bytes())?;
        
        self.file.sync_data()?;
        Ok(())
    }

    pub fn batch_set(&mut self, entries: &[(String, String)]) -> io::Result<()> {
        for (key, value) in entries {
            self.file.write_all(&[OP_SET])?;
            
            let key_len = key.len() as u32;
            self.file.write_all(&key_len.to_le_bytes())?;
            self.file.write_all(key.as_bytes())?;

            let val_len = value.len() as u32;
            self.file.write_all(&val_len.to_le_bytes())?;
            self.file.write_all(value.as_bytes())?;
        }
        self.file.sync_data()?;
        Ok(())
    }
    
    pub fn clear(&mut self) -> io::Result<()> {
        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        Ok(())
    }
}

pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for WALIterator {
    type Item = io::Result<(String, Option<String>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut op_buf = [0u8; 1];
        match self.reader.read_exact(&mut op_buf) {
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e)),
        }
        
        let op = op_buf[0];
        
        // Read Key
        let mut len_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut len_buf) { return Some(Err(e)); }
        let key_len = u32::from_le_bytes(len_buf);
        
        let mut key_buf = vec![0u8; key_len as usize];
        if let Err(e) = self.reader.read_exact(&mut key_buf) { return Some(Err(e)); }
        let key = match String::from_utf8(key_buf) {
            Ok(k) => k,
            Err(e) => return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
        };

        match op {
            OP_SET => {
                // Read Value
                if let Err(e) = self.reader.read_exact(&mut len_buf) { return Some(Err(e)); }
                let val_len = u32::from_le_bytes(len_buf);
                
                let mut val_buf = vec![0u8; val_len as usize];
                if let Err(e) = self.reader.read_exact(&mut val_buf) { return Some(Err(e)); }
                let val = match String::from_utf8(val_buf) {
                    Ok(v) => v,
                    Err(e) => return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
                };
                
                Some(Ok((key, Some(val))))
            }
            OP_DEL => {
                Some(Ok((key, None)))
            }
            _ => Some(Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid opcode"))),
        }
    }
}
