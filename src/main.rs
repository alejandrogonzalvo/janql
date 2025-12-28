use janql::Database;

fn main() {
    let mut db = Database::new("example.db");
    
    db.set("key1".to_string(), "value1".to_string());
    
    match db.get("key1") {
        Some(value) => println!("Retrieved value: {}", value),
        None => println!("Key not found"),
    }
    
    // db.del("key1");
    
    db.flush();
    
    let _loaded_db = Database::load("example.db").expect("Failed to load database");
}
