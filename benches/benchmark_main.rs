use criterion::{criterion_group, criterion_main, Criterion};
use janql::Database;
use tempfile::tempdir;

fn write_benchmark(c: &mut Criterion) {
    c.bench_function("write 100", |b| {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("bench_write.db");
        let mut db = Database::new(&db_path);
        let mut i = 0;
        
        b.iter(|| {
            db.set(format!("key{}", i), "value".to_string());
            i += 1;
        });
    });
}

fn read_benchmark(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bench_read.db");
    let mut db = Database::new(&db_path);
    
    // Pre-fill
    for i in 0..1000 {
        db.set(format!("key{}", i), "value".to_string());
    }

    c.bench_function("read existing", |b| {
        let mut i = 0;
        b.iter(|| {
            db.get(&format!("key{}", i % 1000));
            i += 1;
        });
    });
}

fn load_benchmark(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bench_load.db");
    
    // Pre-fill a DB to load
    {
        let mut db = Database::new(&db_path);
        for i in 0..10000 {
            db.set(format!("key{}", i), "value".to_string());
        }
    } // db dropped

    c.bench_function("load 10k items", |b| {
        b.iter(|| {
            let _ = Database::load(&db_path).unwrap();
        });
    });
}

criterion_group!(benches, write_benchmark, read_benchmark, load_benchmark);
criterion_main!(benches);
