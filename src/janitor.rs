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

    let p1_dirs = match std::fs::read_dir(root) {
        Ok(d) => d,
        Err(_) => return,
    };

    for p1_entry in p1_dirs.flatten() {
        let p1_path = p1_entry.path();
        if !p1_path.is_dir() {
            continue;
        }

        let p1_name = p1_entry.file_name();
        let p1_str = p1_name.to_string_lossy();

        let shard_id = match u8::from_str_radix(&p1_str, 16) {
            Ok(id) => id,
            Err(_) => continue,
        };

        let _lock = shards.write(shard_id);

        let Ok(p2_dirs) = std::fs::read_dir(&p1_path) else {
            continue;
        };

        for p2_entry in p2_dirs.flatten() {
            let p2_path = p2_entry.path();
            if !p2_path.is_dir() {
                continue;
            }

            let Ok(files) = std::fs::read_dir(&p2_path) else {
                continue;
            };

            for file_entry in files.flatten() {
                let file_path = file_entry.path();
                if !file_path.is_file() {
                    continue;
                }

                if let Ok(true) = is_file_expired(&file_path, now_ts) {
                    let _ = std::fs::remove_file(file_path);
                }
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
