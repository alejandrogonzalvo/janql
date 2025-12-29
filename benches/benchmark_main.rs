use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use janql::Database;
use tempfile::tempdir;
use std::path::Path;

fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_ops");
    
    for size in [100, 1000, 10000].iter() {
        // Write Benchmark: Measure latency of `set` when DB has `size` items
        group.bench_with_input(BenchmarkId::new("write", size), size, |b, &s| {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("bench_write.db");
            let mut db = Database::new(&db_path);
            
            // Pre-fill to `size`
            let entries: Vec<(String, String)> = (0..s)
                .map(|i| (format!("key{}", i), "value".to_string()))
                .collect();
            db.batch_set(entries);
            
            let mut i = s;
            b.iter(|| {
                db.set(format!("key{}", i), "value".to_string());
                i += 1;
            });
        });

        // Read Benchmark: Measure latency of `get` when DB has `size` items
        group.bench_with_input(BenchmarkId::new("read", size), size, |b, &s| {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("bench_read.db");
            let mut db = Database::new(&db_path);
            
            // Pre-fill
            let entries: Vec<(String, String)> = (0..s)
                .map(|i| (format!("key{}", i), "value".to_string()))
                .collect();
            db.batch_set(entries);
            
            let mut i = 0;
            b.iter(|| {
                db.get(&format!("key{}", i % s));
                i += 1;
            });
        });

        // Load Benchmark: Measure time to `load` a DB of `size` items
        group.bench_with_input(BenchmarkId::new("load", size), size, |b, &s| {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("bench_load.db");
            
            // Pre-fill
            {
                let mut db = Database::new(&db_path);
                let entries: Vec<(String, String)> = (0..s)
                    .map(|i| (format!("key{}", i), "value".to_string()))
                    .collect();
                db.batch_set(entries);
            }

            b.iter(|| {
                let _ = Database::load(&db_path).unwrap();
            });
        });

        // Get by Prefix Benchmark: Measure latency of `get_by_prefix`
        group.bench_with_input(BenchmarkId::new("get_by_prefix", size), size, |b, &s| {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("bench_prefix.db");
            let mut db = Database::new(&db_path);
            
            // Pre-fill
            let entries: Vec<(String, String)> = (0..s)
                .map(|i| (format!("key{}", i), "value".to_string()))
                .collect();
            db.batch_set(entries);
            
            b.iter(|| {
                db.get_by_prefix("key1");
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().output_directory(Path::new("benches/baselines"));
    targets = benchmarks
}
criterion_main!(benches);
