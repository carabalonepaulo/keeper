use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::Receiver;

use crate::{error::Error, shards::Shards};

type GetCallback = Box<dyn FnOnce(Result<Vec<u8>, Error>) + Send + Sync + 'static>;
type Callback = Box<dyn FnOnce(Result<(), Error>) + Send + Sync + 'static>;

pub enum InputMessage {
    Get {
        path: Arc<PathBuf>,
        key: String,
        callback: GetCallback,
    },
    Set {
        path: Arc<PathBuf>,
        key: String,
        value: Vec<u8>,
        duration: Option<Duration>,
        callback: Callback,
    },
    Remove {
        path: Arc<PathBuf>,
        key: String,
        callback: Callback,
    },
    Clear {
        path: Arc<PathBuf>,
        callback: Callback,
    },
    Quit,
}

pub fn worker(shards: Shards, input_receiver: Receiver<InputMessage>) {
    while let Ok(msg) = input_receiver.recv() {
        match msg {
            InputMessage::Get {
                path,
                key,
                callback,
            } => callback(get(&shards, path, key)),
            InputMessage::Set {
                path,
                key,
                value,
                duration,
                callback,
            } => callback(set(&shards, path, key, value, duration)),
            InputMessage::Remove {
                path,
                key,
                callback,
            } => callback(remove(&shards, path, key)),
            InputMessage::Clear { path, callback } => callback(clear(&shards, path)),
            InputMessage::Quit => break,
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn hash(input: &str) -> Vec<u8> {
    let n = xxhash_rust::xxh3::xxh3_128(input.as_bytes());
    let mut buf = vec![0u8; 32];
    faster_hex::hex_encode(&n.to_be_bytes(), &mut buf).unwrap();
    buf
}

fn get(shards: &Shards, path: Arc<PathBuf>, key: String) -> Result<Vec<u8>, Error> {
    let h = hash(&key);
    let p1 = unsafe { std::str::from_utf8_unchecked(&h[0..2]) };
    let p2 = unsafe { std::str::from_utf8_unchecked(&h[2..4]) };
    let filename = unsafe { std::str::from_utf8_unchecked(&h[4..]) };

    let shard_id = u8::from_str_radix(p1, 16).unwrap_or(0);
    let file_path = path.join(p1).join(p2).join(filename);

    let _lock = shards.read(shard_id);

    let mut file = std::fs::File::open(&file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    if buffer.len() < 10 {
        drop(_lock);
        remove_with_hash(&h, shards, path)?;
        return Err(Error::InvalidData);
    }

    let expires_at = u64::from_be_bytes(buffer[2..10].try_into().unwrap());

    if expires_at != 0 && expires_at < now() {
        drop(_lock);
        remove_with_hash(&h, shards, path)?;
        return Err(Error::NotFound);
    }

    Ok(buffer[10..].to_vec())
}

fn set(
    shards: &Shards,
    path: Arc<PathBuf>,
    key: String,
    value: Vec<u8>,
    duration: Option<Duration>,
) -> Result<(), Error> {
    let h = hash(&key);
    let p1 = unsafe { std::str::from_utf8_unchecked(&h[0..2]) };
    let p2 = unsafe { std::str::from_utf8_unchecked(&h[2..4]) };
    let filename = unsafe { std::str::from_utf8_unchecked(&h[4..]) };

    let shard_id = u8::from_str_radix(p1, 16).unwrap_or(0);
    let folder = path.join(p1).join(p2);
    let file_path = folder.join(filename);

    let expires_at = duration.map(|d| now() + d.as_secs()).unwrap_or(0);

    let _lock = shards.write(shard_id);

    if !folder.exists() {
        std::fs::create_dir_all(&folder)?;
    }

    let mut file = std::fs::File::create(file_path)?;
    file.write_all(&0u16.to_be_bytes())?;
    file.write_all(&expires_at.to_be_bytes())?;
    file.write_all(&value)?;

    Ok(())
}

fn remove(shards: &Shards, path: Arc<PathBuf>, key: String) -> Result<(), Error> {
    let h = hash(&key);
    remove_with_hash(&h, shards, path)
}

fn clear(shards: &Shards, path: Arc<PathBuf>) -> Result<(), Error> {
    let mut locks = Vec::with_capacity(256);
    for i in 0..256 {
        locks.push(shards.write(i as u8));
    }

    if path.exists() {
        std::fs::remove_dir_all(&*path)?;
    }
    std::fs::create_dir_all(&*path)?;

    Ok(())
}

fn remove_with_hash(h: &[u8], shards: &Shards, path: Arc<PathBuf>) -> Result<(), Error> {
    let p1 = unsafe { std::str::from_utf8_unchecked(&h[0..2]) };
    let p2 = unsafe { std::str::from_utf8_unchecked(&h[2..4]) };
    let filename = unsafe { std::str::from_utf8_unchecked(&h[4..]) };

    let shard_id = u8::from_str_radix(p1, 16).unwrap_or(0);
    let file_path = path.join(p1).join(p2).join(filename);

    let _lock = shards.write(shard_id);
    if file_path.exists() {
        std::fs::remove_file(file_path)?;
    }
    Ok(())
}
