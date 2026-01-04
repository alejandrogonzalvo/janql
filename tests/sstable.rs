use janql::sstable::{SSTableBuilder, SSTableReader, SearchResult};
use std::collections::BTreeMap;
use tempfile::TempDir;

#[test]
fn test_sstable_segmentation() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let sst_path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&sst_path).expect("Failed to create builder");

    // Generate data > 4KB
    // 4KB = 4096 bytes.
    // Let's use 100 byte keys+values. 50 entries = 5000 bytes.
    let mut data = BTreeMap::new();
    for i in 0..100 {
        let key = format!("key{:04}", i); // key0000, key0001...
        let value = "v".repeat(100); // 100 chars
        data.insert(key.clone(), value.clone());
        builder.add(&key, &value).expect("Failed to add");
    }

    builder.finish().expect("Failed to finish");

    // Read back
    let mut reader = SSTableReader::new(&sst_path).expect("Failed to open reader");

    // Check index size (should have multiple blocks)
    // We don't expose index size directly but we can check if we can read all keys.
    println!("Index size: {}", reader.index.len());
    assert!(reader.index.len() > 1, "Should have multiple blocks");

    // Verify all keys
    for (key, value) in &data {
        let res = reader.get(key).expect("Failed to get");
        assert_eq!(res, SearchResult::Found(value.clone()));
    }

    // Verify range scan
    let start = "key0010";
    let end = "key0020";
    let range_res = reader.scan(start, end).expect("Failed to scan");

    assert_eq!(range_res.len(), 11); // 10 to 20 inclusive
    for (k, v) in range_res {
        assert_eq!(v, *data.get(&k).unwrap());
    }
}
