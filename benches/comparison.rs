use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use janql::Database;
use tempfile::tempdir;
use rand::prelude::*;

fn comparison_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison_1m");
    group.sample_size(10); // Reduce samples for large benchmarks
    
    let size = 1_000_000;

    // --- WRITE BENCHMARKS ---
    
    group.bench_function("janql_write_1m", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("janql_1m.db");
                (dir, path) // Keep dir alive
            },
            |(_dir, path)| {
                let mut db = Database::new(&path);
                for i in 0..size {
                    db.set(format!("key{}", i), "value".to_string());
                }
            }
        );
    });

    group.bench_function("sled_write_1m", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("sled_1m");
                (dir, path)
            },
            |(_dir, path)| {
                let db = sled::open(&path).unwrap();
                for i in 0..size {
                    let key = format!("key{}", i);
                    db.insert(key.as_bytes(), "value".as_bytes()).unwrap();
                }
                db.flush().unwrap();
            }
        );
    });

    // --- READ BENCHMARKS ---
    
    // JanQL Read Setup
    let janql_dir = tempdir().unwrap();
    let janql_path = janql_dir.path().join("janql_read.db");
    {
        let mut db = Database::new(&janql_path);
        for i in 0..size {
            db.set(format!("key{}", i), "value".to_string());
        }
    }
    // Re-open for reading
    let janql_db = Database::load(&janql_path).unwrap();

    group.bench_function("janql_read_random", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let i = rng.gen_range(0..size);
            janql_db.get(&format!("key{}", i));
        });
    });

    // Sled Read Setup
    let sled_dir = tempdir().unwrap();
    let sled_path = sled_dir.path().join("sled_read");
    let sled_db = sled::open(&sled_path).unwrap();
    for i in 0..size {
        let key = format!("key{}", i);
        sled_db.insert(key.as_bytes(), "value".as_bytes()).unwrap();
    }
    sled_db.flush().unwrap();

    group.bench_function("sled_read_random", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let i = rng.gen_range(0..size);
            let key = format!("key{}", i);
            sled_db.get(key.as_bytes()).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, comparison_benchmarks);
criterion_main!(benches);
