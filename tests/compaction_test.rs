use janql::Database;
use tempfile::tempdir;
use std::fs;

#[test]
fn test_compaction_basic() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db");
    let mut db = Database::new(&db_path);

    // 1. Write "key1" -> "val1", Flush (SSTable 1)
    db.set("key1".to_string(), "val1".to_string());
    db.flush();

    // 2. Write "key1" -> "val2" (update), Flush (SSTable 2)
    db.set("key1".to_string(), "val2".to_string());
    db.flush();

    // 3. Write "key2" -> "val3", Flush (SSTable 3)
    db.set("key2".to_string(), "val3".to_string());
    db.flush();

    // Verify before compaction
    assert_eq!(db.get("key1"), Some("val2".to_string()));
    assert_eq!(db.get("key2"), Some("val3".to_string()));

    // Check file count (should be >= 3 sstables + wal)
    let count = fs::read_dir(&db_path).unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().map_or(false, |ext| ext == "sst"))
        .count();
    assert_eq!(count, 3);

    // 4. Compact
    db.compact().unwrap();

    // Verify after compaction
    assert_eq!(db.get("key1"), Some("val2".to_string()));
    assert_eq!(db.get("key2"), Some("val3".to_string()));

    // Check file count (should be 1 sstable)
    let count_after = fs::read_dir(&db_path).unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().map_or(false, |ext| ext == "sst"))
        .count();
    assert_eq!(count_after, 1);
}

#[test]
fn test_compaction_tombstones() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db_tomb");
    let mut db = Database::new(&db_path);

    // 1. Write "a" -> "1", Flush
    db.set("a".to_string(), "1".to_string());
    db.flush();

    // 2. Delete "a", Flush
    db.del("a");
    db.flush();
    
    eprintln!("Verifying pre-compaction state");
    assert_eq!(db.get("a"), None);
    
    eprintln!("Starting compaction");

    // 3. Compact
    db.compact().unwrap();

    assert_eq!(db.get("a"), None);
}

#[test]
fn test_compaction_mixed() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db_mixed");
    let mut db = Database::new(&db_path);

    db.set("k1".to_string(), "v1".to_string());
    db.flush();
    db.set("k2".to_string(), "v2".to_string());
    db.flush();
    db.set("k1".to_string(), "v1_updated".to_string());
    db.flush();
    db.del("k2");
    db.flush();
    db.set("k3".to_string(), "v3".to_string());
    db.flush();

    // Before compaction
    assert_eq!(db.get("k1"), Some("v1_updated".to_string()));
    assert_eq!(db.get("k2"), None);
    assert_eq!(db.get("k3"), Some("v3".to_string()));

    db.compact().unwrap();

    // After compaction
    assert_eq!(db.get("k1"), Some("v1_updated".to_string()));
    assert_eq!(db.get("k2"), None);
    assert_eq!(db.get("k3"), Some("v3".to_string()));
}
