use std::collections::BTreeMap;

pub struct MemTable {
    map: BTreeMap<String, Option<String>>, // Option<String> allows representing deletions (tombstones)
    size_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        let key_len = key.len();
        let val_len = value.len();
        
        if let Some(old_val) = self.map.insert(key, Some(value)) {
            if let Some(v) = old_val {
                if self.size_bytes >= v.len() {
                    self.size_bytes -= v.len();
                }
            }
        } else {
            self.size_bytes += key_len;
        }
        self.size_bytes += val_len;
    }

    pub fn get(&self, key: &str) -> Option<Option<String>> {
        self.map.get(key).cloned()
    }

    pub fn del(&mut self, key: String) {
        let key_len = key.len();
        if self.map.insert(key, None).is_none() {
             self.size_bytes += key_len;
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Option<String>> {
        self.map.iter()
    }
    
    pub fn clear(&mut self) {
        self.map.clear();
        self.size_bytes = 0;
    }
}
