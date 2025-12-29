use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const BLOCK_SIZE: usize = 4 * 1024; // 4KB

#[derive(Debug)]
pub struct SSTableBuilder {
    file: File,
    block_buffer: Vec<u8>,
    index: BTreeMap<String, u64>, // StartKey -> Offset
    current_offset: u64,
    first_key_in_block: Option<String>,
}

impl SSTableBuilder {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        Ok(Self {
            file,
            block_buffer: Vec::with_capacity(BLOCK_SIZE),
            index: BTreeMap::new(),
            current_offset: 0,
            first_key_in_block: None,
        })
    }

    pub fn add(&mut self, key: &str, value: &str) -> io::Result<()> {
        let entry_size = 4 + key.len() + 4 + value.len();
        
        // If adding this entry would exceed block size (and buffer is not empty), flush first
        if !self.block_buffer.is_empty() && self.block_buffer.len() + entry_size > BLOCK_SIZE {
            self.flush_block()?;
        }

        if self.first_key_in_block.is_none() {
            self.first_key_in_block = Some(key.to_string());
        }

        self.write_entry_to_buffer(key, value);

        Ok(())
    }

    fn write_entry_to_buffer(&mut self, key: &str, value: &str) {
        let key_len = key.len() as u32;
        self.block_buffer.extend_from_slice(&key_len.to_le_bytes());
        self.block_buffer.extend_from_slice(key.as_bytes());

        let val_len = value.len() as u32;
        self.block_buffer.extend_from_slice(&val_len.to_le_bytes());
        self.block_buffer.extend_from_slice(value.as_bytes());
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.block_buffer.is_empty() {
            return Ok(());
        }

        // Record index
        if let Some(key) = &self.first_key_in_block {
            self.index.insert(key.clone(), self.current_offset);
        }

        // Write buffer to file
        self.file.write_all(&self.block_buffer)?;
        self.current_offset += self.block_buffer.len() as u64;

        // Reset
        self.block_buffer.clear();
        self.first_key_in_block = None;

        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        self.flush_block()?;

        let index_offset = self.current_offset;

        // Write index
        for (key, offset) in &self.index {
            let key_len = key.len() as u32;
            self.file.write_all(&key_len.to_le_bytes())?;
            self.file.write_all(key.as_bytes())?;
            self.file.write_all(&offset.to_le_bytes())?;
        }

        // Write footer: index_offset (8 bytes)
        self.file.write_all(&index_offset.to_le_bytes())?;

        self.file.sync_all()?;
        Ok(())
    }
}

pub struct SSTableReader {
    file: File,
    pub index: BTreeMap<String, u64>,
}

impl SSTableReader {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();

        if len < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "File too short"));
        }

        // Read footer
        file.seek(SeekFrom::End(-8))?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let index_offset = u64::from_le_bytes(buf);

        // Read index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index = BTreeMap::new();
        
        // Read until we hit the footer (last 8 bytes)
        let index_len = len - 8 - index_offset;
        
        let mut index_data = vec![0u8; index_len as usize];
        file.seek(SeekFrom::Start(index_offset))?;
        file.read_exact(&mut index_data)?;
        
        let mut cursor = std::io::Cursor::new(index_data);
        while cursor.position() < index_len {
            // Read key len
            let mut len_buf = [0u8; 4];
            if cursor.read_exact(&mut len_buf).is_err() { break; }
            let key_len = u32::from_le_bytes(len_buf);

            // Read key
            let mut key_buf = vec![0u8; key_len as usize];
            cursor.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // Read offset
            let mut off_buf = [0u8; 8];
            cursor.read_exact(&mut off_buf)?;
            let offset = u64::from_le_bytes(off_buf);

            index.insert(key, offset);
        }

        Ok(Self { file, index })
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        let block_offset = self.index.range(..=key.to_string()).next_back().map(|(_, &off)| off);

        if let Some(offset) = block_offset {
            self.search_in_block(offset, key)
        } else {
            Ok(None)
        }
    }

    fn search_in_block(&mut self, offset: u64, key: &str) -> io::Result<Option<String>> {
        self.file.seek(SeekFrom::Start(offset))?;
        
        let _next_offset = self.index.range((std::ops::Bound::Excluded(key.to_string()), std::ops::Bound::Unbounded))
            .next()
            .map(|(_, &off)| off)
            .unwrap_or_else(|| {
                u64::MAX // Placeholder
            });

        let _file_len = self.file.metadata()?.len();
        let _footer_len = 8;
        
        loop {
            let _current_pos = self.file.stream_position()?;
            
            // Read key len
            let mut len_buf = [0u8; 4];
            if self.file.read_exact(&mut len_buf).is_err() { break; }
            let k_len = u32::from_le_bytes(len_buf);

            // Read key
            let mut k_buf = vec![0u8; k_len as usize];
            self.file.read_exact(&mut k_buf)?;
            let k = String::from_utf8(k_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // Read val len
            let mut v_len_buf = [0u8; 4];
            self.file.read_exact(&mut v_len_buf)?;
            let v_len = u32::from_le_bytes(v_len_buf);

            if k == key {
                // Found it
                let mut v_buf = vec![0u8; v_len as usize];
                self.file.read_exact(&mut v_buf)?;
                let v = String::from_utf8(v_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                return Ok(Some(v));
            } else if k > key.to_string() {
                // Passed it
                return Ok(None);
            }

            // Skip value
            self.file.seek(SeekFrom::Current(v_len as i64))?;
        }

        Ok(None)
    }
    
    pub fn scan(&mut self, start: &str, end: &str) -> io::Result<Vec<(String, String)>> {
        let mut results = Vec::new();
        
        // Find starting block
        let start_offset = self.index.range(..=start.to_string()).next_back().map(|(_, &off)| off).unwrap_or(0);
        
        self.file.seek(SeekFrom::Start(start_offset))?;
        
        loop {
             // Read key len
            let mut len_buf = [0u8; 4];
            if self.file.read_exact(&mut len_buf).is_err() { break; }
            let k_len = u32::from_le_bytes(len_buf);

            // Read key
            let mut k_buf = vec![0u8; k_len as usize];
            self.file.read_exact(&mut k_buf)?;
            let k = String::from_utf8(k_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // Read val len
            let mut v_len_buf = [0u8; 4];
            self.file.read_exact(&mut v_len_buf)?;
            let v_len = u32::from_le_bytes(v_len_buf);
            
            if k >= start.to_string() && k <= end.to_string() {
                 let mut v_buf = vec![0u8; v_len as usize];
                self.file.read_exact(&mut v_buf)?;
                let v = String::from_utf8(v_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                results.push((k, v));
            } else if k > end.to_string() {
                // Done
                break;
            } else {
                 // Skip value
                self.file.seek(SeekFrom::Current(v_len as i64))?;
            }
        }
        
        Ok(results)
    }
}
