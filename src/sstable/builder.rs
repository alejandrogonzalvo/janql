use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use super::{BLOCK_SIZE, TOMBSTONE};

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
        let val_len = value.len();
        let entry_size = 4 + key.len() + 4 + val_len;

        // If adding this entry would exceed block size (and buffer is not empty), flush first
        if !self.block_buffer.is_empty() && self.block_buffer.len() + entry_size > BLOCK_SIZE {
            self.flush_block()?;
        }

        if self.first_key_in_block.is_none() {
            self.first_key_in_block = Some(key.to_string());
        }

        self.write_entry_to_buffer(key, Some(value));

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        let entry_size = 4 + key.len() + 4; // val_len (4) + (0 bytes payload)

        if !self.block_buffer.is_empty() && self.block_buffer.len() + entry_size > BLOCK_SIZE {
            self.flush_block()?;
        }

        if self.first_key_in_block.is_none() {
            self.first_key_in_block = Some(key.to_string());
        }

        self.write_entry_to_buffer(key, None);
        Ok(())
    }

    fn write_entry_to_buffer(&mut self, key: &str, value: Option<&str>) {
        let key_len = key.len() as u32;
        self.block_buffer.extend_from_slice(&key_len.to_le_bytes());
        self.block_buffer.extend_from_slice(key.as_bytes());

        match value {
            Some(v) => {
                let val_len = v.len() as u32;
                self.block_buffer.extend_from_slice(&val_len.to_le_bytes());
                self.block_buffer.extend_from_slice(v.as_bytes());
            }
            None => {
                self.block_buffer
                    .extend_from_slice(&TOMBSTONE.to_le_bytes());
            }
        }
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
