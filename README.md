# Keeper

Keeper is a sharded, file-based cache for Rust, designed for safe concurrent
access and predictable background cleanup. It offloads I/O operations to
background workers and uses a directory-based locking strategy to allow multiple
operations to proceed in parallel when possible.

## Characteristics

- **Sharded Locking**: The cache directory is split into 4096 subfolders, based
  on the first 3 characters of the key's XXH3-128 hash. Each folder is guarded
  by an independent `RwLock`, allowing operations on different shards to run
  concurrently.
- **Shallow Storage**: Data is stored one level deep (`root/abc/file`). This
  keeps directory depth low, reducing filesystem overhead while maintaining a
  manageable number of files per folder.
- **Non-blocking Cleanup**: A background janitor removes expired files. It tries
  to acquire locks on each shard; if a shard is currently being accessed, the
  janitor skips it. This ensures cleanup does not block ongoing store
  operations.
- **Worker Model**: Store operations are dispatched to a thread pool via
  channels. If a worker panics, the error is returned to the caller, preventing
  requests from hanging indefinitely.

## Features

Keeper supports three API modes via feature flags:

1. **Callbacks (Default)**: Requests are sent with a completion closure
2. **`sync`**: Blocking API where methods return `Result` directly
3. **`async`**: Integration with Tokio using `oneshot` channels

## Usage

```rust
use keeper::{KeeperBuilder, Error};
use std::time::Duration;

async fn run() -> Result<(), Error> {
    let cache = KeeperBuilder::new("./cache".into())
        .with_cleanup_interval(Duration::from_secs(60))
        .build()?;

    cache.set("key", b"value".to_vec(), Some(Duration::from_secs(3600))).await?;
    let data = cache.get("key").await?;
    
    Ok(())
}
```

## Internal Layout

Keys are mapped to subdirectories using the first 3 characters of their XXH3-128
hash:

- **Hash**: `a1b2c3...`
- **Path**: `root/a1b/2c3...`
- **Lock**: Shard `0xa1b` (4096 total)

This 1:1 mapping between subdirectories and locks ensures that operations on one
shard do not block unrelated shards, improving concurrency.

## Implementation Details

- **Header**: Each file contains a 10-byte header: 2 bytes for
  version/placeholder and 8 bytes for a Big-Endian expiration timestamp.
- **Safety**: Uses `Pidlock` to prevent multiple processes from accessing the
  same cache directory at the same time.
