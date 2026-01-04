use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use super::TOMBSTONE;

#[derive(Debug, PartialEq, Eq)]
pub enum SearchResult {
    Found(String),
    NotFound,
    Deleted,
}

pub struct SSTableReader {
    pub(crate) file: File,
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
            if cursor.read_exact(&mut len_buf).is_err() {
                break;
            }
            let key_len = u32::from_le_bytes(len_buf);

            // Read key
            let mut key_buf = vec![0u8; key_len as usize];
            cursor.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // Read offset
            let mut off_buf = [0u8; 8];
            cursor.read_exact(&mut off_buf)?;
            let offset = u64::from_le_bytes(off_buf);

            index.insert(key, offset);
        }

        Ok(Self { file, index })
    }

    pub fn get(&mut self, key: &str) -> io::Result<SearchResult> {
        let block_offset = self
            .index
            .range(..=key.to_string())
            .next_back()
            .map(|(_, &off)| off);

        if let Some(offset) = block_offset {
            self.search_in_block(offset, key)
        } else {
            Ok(SearchResult::NotFound)
        }
    }

    fn search_in_block(&mut self, offset: u64, key: &str) -> io::Result<SearchResult> {
        self.file.seek(SeekFrom::Start(offset))?;

        loop {
            // Read key len
            let mut len_buf = [0u8; 4];
            if self.file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let k_len = u32::from_le_bytes(len_buf);

            // Read key
            let mut k_buf = vec![0u8; k_len as usize];
            self.file.read_exact(&mut k_buf)?;
            let k = String::from_utf8(k_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // Read val len
            let mut v_len_buf = [0u8; 4];
            self.file.read_exact(&mut v_len_buf)?;
            let v_len = u32::from_le_bytes(v_len_buf);

            if k == key {
                if v_len == TOMBSTONE {
                    return Ok(SearchResult::Deleted); // Tombstone explicitly found
                }
                let mut v_buf = vec![0u8; v_len as usize];
                self.file.read_exact(&mut v_buf)?;
                let v = String::from_utf8(v_buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                return Ok(SearchResult::Found(v));
            } else if k > key.to_string() {
                // Passed it
                return Ok(SearchResult::NotFound);
            }

            // Skip value
            if v_len != TOMBSTONE {
                self.file.seek(SeekFrom::Current(v_len as i64))?;
            }
        }

        Ok(SearchResult::NotFound)
    }

    pub fn scan(&mut self, start: &str, end: &str) -> io::Result<Vec<(String, String)>> {
        let mut results = Vec::new();
        let start_offset = self
            .index
            .range(..=start.to_string())
            .next_back()
            .map(|(_, &off)| off)
            .unwrap_or(0);
        self.file.seek(SeekFrom::Start(start_offset))?;

        loop {
            // Read key len
            let mut len_buf = [0u8; 4];
            if self.file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let k_len = u32::from_le_bytes(len_buf);

            // Read key
            let mut k_buf = vec![0u8; k_len as usize];
            self.file.read_exact(&mut k_buf)?;
            let k = String::from_utf8(k_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // Read val len
            let mut v_len_buf = [0u8; 4];
            self.file.read_exact(&mut v_len_buf)?;
            let v_len = u32::from_le_bytes(v_len_buf);

            if k >= start.to_string() && k <= end.to_string() {
                if v_len != TOMBSTONE {
                    let mut v_buf = vec![0u8; v_len as usize];
                    self.file.read_exact(&mut v_buf)?;
                    let v = String::from_utf8(v_buf)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    results.push((k, v));
                }
            } else if k > end.to_string() {
                // Done
                break;
            } else {
                // Skip value
                if v_len != TOMBSTONE {
                    self.file.seek(SeekFrom::Current(v_len as i64))?;
                }
            }
        }

        Ok(results)
    }
}

pub struct SSTableIterator {
    reader: SSTableReader,
    current_offset: u64,
    end_offset: u64,
}

impl IntoIterator for SSTableReader {
    type Item = io::Result<(String, Option<String>)>;
    type IntoIter = SSTableIterator;

    fn into_iter(mut self) -> Self::IntoIter {
        // Find the start of the index (end_offset for data)
        let end_offset = (|| {
            let len = self.file.metadata().ok()?.len();
            if len < 8 {
                return None;
            }

            self.file.seek(SeekFrom::End(-8)).ok()?;
            let mut buf = [0u8; 8];
            self.file.read_exact(&mut buf).ok()?;

            Some(u64::from_le_bytes(buf))
        })()
        .unwrap_or(u64::MAX);

        // Reset to start
        let _ = self.file.seek(SeekFrom::Start(0));

        SSTableIterator {
            reader: self,
            current_offset: 0,
            end_offset,
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = io::Result<(String, Option<String>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end_offset == u64::MAX {
            return None;
        } // Invalid state
        if self.current_offset >= self.end_offset {
            return None;
        }

        let mut len_buf = [0u8; 4];
        if let Err(_) = self.reader.file.read_exact(&mut len_buf) {
            return None;
        }
        let key_len = u32::from_le_bytes(len_buf);

        let mut key_buf = vec![0u8; key_len as usize];
        if let Err(e) = self.reader.file.read_exact(&mut key_buf) {
            return Some(Err(e));
        }
        let key = match String::from_utf8(key_buf) {
            Ok(k) => k,
            Err(e) => return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
        };

        let mut v_len_buf = [0u8; 4];
        if let Err(e) = self.reader.file.read_exact(&mut v_len_buf) {
            return Some(Err(e));
        }
        let val_len = u32::from_le_bytes(v_len_buf);

        let val = if val_len == TOMBSTONE {
            None
        } else {
            let mut v_buf = vec![0u8; val_len as usize];
            if let Err(e) = self.reader.file.read_exact(&mut v_buf) {
                return Some(Err(e));
            }
            match String::from_utf8(v_buf) {
                Ok(v) => Some(v),
                Err(e) => return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
            }
        };

        self.current_offset = self
            .reader
            .file
            .stream_position()
            .unwrap_or(self.current_offset);

        Some(Ok((key, val)))
    }
}
