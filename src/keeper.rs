use std::{path::PathBuf, sync::Arc, thread::JoinHandle, time::Duration};

use crossbeam::channel::{Sender, unbounded};
use pidlock::Pidlock;

use crate::{error::Error, janitor, shards::Shards, store};

struct Inner {
    path: Arc<PathBuf>,
    _lock: Pidlock,

    store_is: Sender<store::InputMessage>,
    janitor_is: Sender<janitor::InputMessage>,

    store_handle: Option<JoinHandle<()>>,
    janitor_handle: Option<JoinHandle<()>>,
}

pub struct Keeper(Arc<Inner>);

impl Keeper {
    pub fn new(path: PathBuf, cleanup_interval: Duration) -> Result<Self, Error> {
        let mut lock = Pidlock::new_validated(path.join(".lock"))?;
        lock.acquire()?;

        let path = Arc::new(path);
        let shards = Shards::new();

        let (store_is, store_ir) = unbounded::<store::InputMessage>();
        let (janitor_is, janitor_ir) = unbounded::<janitor::InputMessage>();

        let store_handle = std::thread::spawn({
            let shards = shards.clone();
            move || store::worker(shards, store_ir)
        });
        let janitor_handle = std::thread::spawn({
            let path = path.clone();
            move || janitor::worker(cleanup_interval, path, shards, janitor_ir)
        });

        let inner = Inner {
            path,
            _lock: lock,

            store_is,
            janitor_is,

            store_handle: Some(store_handle),
            janitor_handle: Some(janitor_handle),
        };

        Ok(Self(Arc::new(inner)))
    }

    pub fn get<F>(&self, key: &str, cb: F)
    where
        F: FnOnce(Result<Vec<u8>, Error>) + Send + Sync + 'static,
    {
        self.0
            .store_is
            .send(store::InputMessage::Get {
                path: self.0.path.clone(),
                key: key.into(),
                callback: Box::new(cb),
            })
            .expect("store worker channel closed unexpectedly");
    }

    pub fn set<F>(&self, key: &str, value: &[u8], duration: Option<Duration>, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.0
            .store_is
            .send(store::InputMessage::Set {
                path: self.0.path.clone(),
                key: key.into(),
                value: value.into(),
                duration,
                callback: Box::new(cb),
            })
            .expect("store worker channel closed unexpectedly");
    }

    pub fn remove<F>(&self, key: &str, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.0
            .store_is
            .send(store::InputMessage::Remove {
                path: self.0.path.clone(),
                key: key.into(),
                callback: Box::new(cb),
            })
            .expect("store worker channel closed unexpectedly");
    }

    pub fn clear<F>(&self, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.0
            .store_is
            .send(store::InputMessage::Clear {
                path: self.0.path.clone(),
                callback: Box::new(cb),
            })
            .expect("store worker channel closed unexpectedly");
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.store_is.send(store::InputMessage::Quit).ok();
        self.janitor_is.send(janitor::InputMessage::Quit).ok();

        if let Some(handle) = self.store_handle.take() {
            handle.join().ok();
        }

        if let Some(handle) = self.janitor_handle.take() {
            handle.join().ok();
        }
    }
}
