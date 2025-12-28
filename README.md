# JanQL

JanQL is a lightweight, persistent key-value store written in Rust. It features an append-only log for durability, JSON serialization, and an in-memory HashMap for fast reads.

## Features

- **Simple API**: `set`, `get`, `del`.
- **Persistence**: Append-only log with crash recovery (`load`).
- **Compaction**: `flush` operation to compact the log.
- **Performance**: In-memory reads, constant-time writes.

## Usage

```rust
use janql::Database;

fn main() {
    let mut db = Database::new("my.db");
    
    db.set("key".to_string(), "value".to_string());
    println!("{:?}", db.get("key"));
    
    db.del("key");
}
```

## Testing & Benchmarking

### Unit Tests
Run the standard test suite to verify correctness:
```bash
cargo test
```

### Benchmarks
We use `criterion` for benchmarking.

**Run Standard Benchmarks:**
Measures write, read, and load performance for 100, 1k, and 10k items.
```bash
cargo bench
```

**Run Comparison Benchmarks:**
Compares JanQL against `sled` (requires `comparison` feature).
```bash
cargo bench --features comparison
```

### Performance Regression Testing
We maintain performance baselines to detect regressions.

**Save a New Baseline:**
To save the current performance as a named baseline (e.g., `v0.1`):
```bash
cargo bench -- --save-baseline v0.1
```

**Compare Against Baseline:**
To compare the current code against a saved baseline:
```bash
cargo bench -- --baseline v0.1
```
