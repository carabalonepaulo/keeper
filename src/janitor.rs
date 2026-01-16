use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crossbeam::channel::{Receiver, RecvTimeoutError};

use crate::{shards::Shards, utils::now};

pub enum InputMessage {
    Quit,
}

pub fn worker(
    interval: Duration,
    path: Arc<PathBuf>,
    shards: Shards,
    input_receiver: Receiver<InputMessage>,
) {
    loop {
        match input_receiver.recv_timeout(interval) {
            Ok(InputMessage::Quit) => break,
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => cleanup(&path, &shards),
        }
    }
}

fn cleanup(root: &Path, shards: &Shards) {
    let now_ts = now();

    let entries = match std::fs::read_dir(root) {
        Ok(d) => d,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let folder_path = entry.path();
        if !folder_path.is_dir() {
            continue;
        }

        let folder_name = entry.file_name();
        let name_str = folder_name.to_string_lossy();

        let shard_id = match u16::from_str_radix(&name_str, 16) {
            Ok(id) => id,
            Err(_) => continue,
        };

        let Ok(_lock) = shards.try_write(shard_id) else {
            continue;
        };

        let Ok(files) = std::fs::read_dir(&folder_path) else {
            continue;
        };

        for file_entry in files.flatten() {
            let file_path = file_entry.path();
            if !file_path.is_file() {
                continue;
            }

            match is_file_expired(&file_path, now_ts) {
                Ok(true) | Err(_) => {
                    let _ = std::fs::remove_file(file_path);
                }
                Ok(false) => continue,
            }
        }
    }
}

fn is_file_expired(path: &Path, now: u64) -> std::io::Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut header = [0u8; 10];

    if file.read_exact(&mut header).is_err() {
        return Ok(true);
    }

    let expires_at = u64::from_be_bytes(header[2..10].try_into().unwrap());
    if expires_at == 0 {
        return Ok(false);
    }

    Ok(expires_at < now)
}
