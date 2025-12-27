use janql::Database;
use rstest::*;
use tempfile::{tempdir, TempDir};
use std::fs;
use std::ops::{Deref, DerefMut};

#[cfg(test)]
mod tests {
    use super::*;

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
        let dir = tempdir().unwrap();
        let db = Database::new(dir.path().join("test.db"));
        TestDb { db, dir }
    }

    #[rstest]
    fn test_set_and_get(mut db: TestDb) {
        db.set("key1".into(), "value1".into());
        db.set("key2".into(), "value2".into());
        assert_eq!(db.get("key1"), Some(&"value1".into()));
        assert_eq!(db.get("key2"), Some(&"value2".into()));
    }

    #[rstest]
    #[should_panic]
    fn test_get_non_existent_key(db: TestDb) {
        db.get("non_existent");
    }

    #[rstest]
    fn test_delete_key(mut db: TestDb) {
        db.set("key1".into(), "value1".into());
        db.del("key1");
    }

    #[rstest]
    #[should_panic]
    fn test_delete_non_existent_key(mut db: TestDb) {
        db.del("non_existent");
    }

    #[rstest]
    fn test_flush_database(mut db: TestDb) {
        db.set("key1".into(), "value1".into());
        db.set("key2".into(), "value2".into());
        db.flush();

        let contents = fs::read_to_string(db.dir.path().join("test.db")).unwrap();
        assert!(contents.contains("SET \"key1\" \"value1\""));
        assert!(contents.contains("SET \"key2\" \"value2\""));
    }

    #[rstest]
    fn test_log_append(mut db: TestDb) {
        db.set("key1".into(), "value1".into());
        let contents = fs::read_to_string(db.dir.path().join("test.db")).unwrap();
        assert!(contents.contains("SET \"key1\" \"value1\""));
        
        db.del("key1");
        let contents = fs::read_to_string(db.dir.path().join("test.db")).unwrap();
        assert!(contents.contains("DEL \"key1\""));
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        {
            let mut db = Database::new(&db_path);
            db.set("key1".into(), "value1".into());
            db.set("key2".into(), "value2".into());
            db.del("key2");
        } // db dropped, file closed

        let loaded_db = Database::load(&db_path).unwrap();
        assert_eq!(loaded_db.get("key1"), Some(&"value1".into()));
        // key2 should be deleted. get panics on missing key, so we expect panic?
        // Or we can check if it's not there. But get panics.
        // Let's verify key1 is there.
    }
    
    #[test]
    #[should_panic]
    fn test_persistence_deleted_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        {
            let mut db = Database::new(&db_path);
            db.set("key1".into(), "value1".into());
            db.del("key1");
        } 

        let loaded_db = Database::load(&db_path).unwrap();
        loaded_db.get("key1");
    }
}