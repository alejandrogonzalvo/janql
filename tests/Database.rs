use janql::Database;
use rstest::{fixture, rstest};
use std::fs;
use std::ops::{Deref, DerefMut};
use tempfile::TempDir;

struct TestDb {
    db: Database,
    dir: TempDir,
}

impl Deref for TestDb {
    type Target = Database;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for TestDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

#[fixture]
fn db() -> TestDb {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = dir.path().join("test.db");
    let db = Database::new(&db_path);
    TestDb { db, dir }
}

#[rstest]
fn test_set_and_get(mut db: TestDb) {
    db.set("key1".to_string(), "value1".to_string());
    assert_eq!(db.get("key1"), Some("value1".to_string()));
}

#[rstest]
fn test_get_non_existent_key(mut db: TestDb) {
    assert_eq!(db.get("non_existent"), None);
}

#[rstest]
fn test_delete_key(mut db: TestDb) {
    db.set("key1".to_string(), "value1".to_string());
    db.del("key1");
    assert_eq!(db.get("key1"), None);
}

#[rstest]
fn test_delete_non_existent_key(mut db: TestDb) {
    // Should not panic
    db.del("non_existent");
}

#[rstest]
fn test_log_append(mut db: TestDb) {
    let initial_size = fs::metadata(&db.path).expect("Unable to read metadata").len();
    
    db.set("key1".to_string(), "value1".to_string());
    let size_after_set = fs::metadata(&db.path).expect("Unable to read metadata").len();
    assert!(size_after_set > initial_size);
    
    db.del("key1");
    let size_after_del = fs::metadata(&db.path).expect("Unable to read metadata").len();
    assert!(size_after_del > size_after_set);
    
    // Verify content by loading
    let mut loaded_db = Database::load(&db.path).expect("Failed to load database");
    assert_eq!(loaded_db.get("key1"), None);
}

#[rstest]
fn test_flush_database(mut db: TestDb) {
    db.set("key1".to_string(), "value1".to_string());
    db.set("key2".to_string(), "value2".to_string());
    db.del("key1");
    
    let size_before = fs::metadata(&db.path).expect("Unable to read metadata").len();
    
    db.flush();

    let size_after = fs::metadata(&db.path).expect("Unable to read metadata").len();
    
    // Size should decrease because key1 and its deletion are removed
    assert!(size_after < size_before);

    let mut loaded_db = Database::load(&db.path).expect("Failed to load database");
    assert_eq!(loaded_db.get("key2"), Some("value2".to_string()));
    assert_eq!(loaded_db.get("key1"), None);
}

#[rstest]
fn test_persistence() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = dir.path().join("test_persistence.db");
    
    {
        let mut db = Database::new(&db_path);
        db.set("key1".to_string(), "value1".to_string());
    } // db dropped here, file closed, but dir persists

    let mut loaded_db = Database::load(&db_path).expect("Failed to load database");
    assert_eq!(loaded_db.get("key1"), Some("value1".to_string()));
}

#[rstest]
fn test_persistence_deleted_key() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = dir.path().join("test_persistence_del.db");
    
    {
        let mut db = Database::new(&db_path);
        db.set("key1".to_string(), "value1".to_string());
        db.del("key1");
    }

    let mut loaded_db = Database::load(&db_path).expect("Failed to load database");
    assert_eq!(loaded_db.get("key1"), None);
}