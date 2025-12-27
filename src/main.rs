use janql::Database;

fn main() {
    let db_path = "example.db";
    
    println!("Creating new database at {}", db_path);
    let mut db = Database::new(db_path);

    println!("Setting key1 = value1");
    db.set("key1".to_string(), "value1".to_string());
    
    println!("Setting key2 = value2");
    db.set("key2".to_string(), "value2".to_string());

    println!("Getting key1: {:?}", db.get("key1"));

    println!("Deleting key1");
    db.del("key1");

    println!("Flushing (compacting) database...");
    db.flush();

    drop(db);

    println!("Reloading database from disk...");
    let db = Database::load(db_path).expect("Failed to load database");

    println!("Getting key2 from loaded database: {:?}", db.get("key2"));
    
    println!("Done!");
}
