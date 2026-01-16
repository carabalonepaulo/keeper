# Keeper

Keeper is a sharded, file-based cache for Rust. It offloads I/O operations to
background workers and uses a directory-based locking strategy to minimize
contention between reads, writes, and maintenance.

## Characteristics

- **Sharded Locking**: The cache directory is split into 4096 subfolders. Each
  folder is guarded by an independent `RwLock`, allowing operations on different
  shards to run in parallel.
- **Shallow Storage**: Data is stored one level deep (`root/abc/file`). This
  prevents deep path resolution overhead while keeping the number of files per
  directory manageable.
- **Non-blocking Cleanup**: A background janitor removes expired files. It uses
  `try_write` locks; if a shard is currently being accessed by a store
  operation, the janitor skips it to avoid introducing latency.
- **Worker Model**: Requests are dispatched to threads via channels. The system
  is designed to return an error if a worker thread panics, preventing the
  caller from hanging.

## Features

Keeper supports three API modes via feature flags:

1. **Callbacks (Default)**: Requests are sent with a completion closure.
2. **`sync`**: A blocking API where methods return `Result` directly.
3. **`async`**: Integration with `Tokio` using `oneshot` channels.

## Usage

```rust
use keeper::{Keeper, Error};
use std::time::Duration;

async fn run() -> Result<(), Error> {
    let cache = Keeper::new("./cache".into(), Duration::from_secs(60))?;

    cache.set("key", b"value".to_vec(), Some(Duration::from_secs(3600))).await?;
    
    let data = cache.get("key").await?;
    Ok(())
}
```

## Internal Layout

The cache maps the first 3 characters of a key's XXH3-128 hash to a
subdirectory:

- **Hash**: `a1b2c3...`
- **Path**: `root/a1b/2c3...`
- **Lock**: Shard `0xa1b` (4096 total).

This 1:1 mapping between subdirectories and locks ensures that maintenance in
one folder does not block access to others.

## Implementation Details

- **Header**: Each file contains a 10-byte header: a 2-byte version/placeholder
  and an 8-byte Big-Endian expiration timestamp.
- **Safety**: Uses `Pidlock` to prevent multiple processes from accessing the
  same cache directory simultaneously.
