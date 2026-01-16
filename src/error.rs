#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    PidLock(#[from] pidlock::PidlockError),
    #[error("cache not found or expired")]
    NotFound,
    #[error("cache data is invalid or corrupted")]
    InvalidData,
    #[error("worker response channel closed")]
    WorkerClosed,
}
