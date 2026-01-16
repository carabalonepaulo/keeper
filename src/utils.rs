use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn parse_hash(h: &[u8]) -> (&str, &str, u16) {
    let p_folder = unsafe { std::str::from_utf8_unchecked(&h[0..3]) };
    let filename = unsafe { std::str::from_utf8_unchecked(&h[3..]) };
    let shard_id = u16::from_str_radix(p_folder, 16).unwrap_or(0);

    (p_folder, filename, shard_id)
}
