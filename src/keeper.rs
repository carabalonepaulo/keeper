use std::{path::PathBuf, sync::Arc, thread::JoinHandle, time::Duration};

use crossbeam::channel::{Sender, unbounded};
use pidlock::Pidlock;

use crate::{error::Error, janitor, shards::Shards, store};

#[cfg(feature = "async")]
use tokio::sync::oneshot;

#[derive(Debug)]
struct Inner {
    path: Arc<PathBuf>,
    _lock: Pidlock,

    store_is: Sender<store::InputMessage>,
    janitor_is: Sender<janitor::InputMessage>,

    store_handles: Vec<JoinHandle<()>>,
    janitor_handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct KeeperBuilder {
    path: PathBuf,
    cleanup_interval: Duration,
    store_workers: usize,
}

impl KeeperBuilder {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cleanup_interval: Duration::from_mins(60),
            store_workers: 1,
        }
    }

    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    pub fn with_store_workers(mut self, count: usize) -> Self {
        self.store_workers = count.max(1);
        self
    }

    pub fn build(self) -> Result<Keeper, Error> {
        Keeper::new_with_builder(self)
    }
}

#[derive(Debug, Clone)]
pub struct Keeper(Arc<Inner>);

impl Keeper {
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        KeeperBuilder::new(path).build()
    }

    pub fn new_with_builder(builder: KeeperBuilder) -> Result<Self, Error> {
        let mut lock = Pidlock::new_validated(builder.path.join(".lock"))?;
        lock.acquire()?;

        let path = Arc::new(builder.path);
        let shards = Shards::new();

        let (store_is, store_ir) = unbounded::<store::InputMessage>();
        let (janitor_is, janitor_ir) = unbounded::<janitor::InputMessage>();

        let mut store_handles = Vec::with_capacity(builder.store_workers);
        for _ in 0..builder.store_workers {
            let handle = std::thread::spawn({
                let shards = shards.clone();
                let ir = store_ir.clone();
                move || store::worker(shards, ir)
            });
            store_handles.push(handle);
        }

        let janitor_handle = std::thread::spawn({
            let path = path.clone();
            move || janitor::worker(builder.cleanup_interval, path, shards, janitor_ir)
        });

        let inner = Inner {
            path,
            _lock: lock,

            store_is,
            janitor_is,

            store_handles,
            janitor_handle: Some(janitor_handle),
        };

        Ok(Self(Arc::new(inner)))
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    pub async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let (tx, rx) = oneshot::channel();
        self.dispatch_get(key, move |res| {
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        duration: Option<Duration>,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.dispatch_set(key, value, duration, move |res| {
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    pub async fn remove(&self, key: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.dispatch_remove(key, move |res| {
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    pub async fn clear(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.dispatch_clear(move |res| {
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    pub async fn cleanup(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.dispatch_cleanup(move |res| {
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch_get(key, move |res| {
            let _ = tx.send(res);
        });
        rx.recv().map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn set(&self, key: &str, value: &[u8], duration: Option<Duration>) -> Result<(), Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch_set(key, value, duration, move |res| {
            let _ = tx.send(res);
        });
        rx.recv().map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn remove(&self, key: &str) -> Result<(), Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch_remove(key, move |res| {
            let _ = tx.send(res);
        });
        rx.recv().map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn clear(&self) -> Result<(), Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch_clear(move |res| {
            let _ = tx.send(res);
        });
        rx.recv().map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn cleanup(&self) -> Result<(), Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch_cleanup(move |res| {
            let _ = tx.send(res);
        });
        rx.recv().map_err(|_| Error::WorkerClosed)?
    }

    #[cfg(all(not(feature = "async"), not(feature = "sync")))]
    pub fn get<F>(&self, key: &str, cb: F)
    where
        F: FnOnce(Result<Vec<u8>, Error>) + Send + Sync + 'static,
    {
        self.dispatch_get(key, cb);
    }

    #[cfg(all(not(feature = "async"), not(feature = "sync")))]
    pub fn set<F>(&self, key: &str, value: &[u8], duration: Option<Duration>, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.dispatch_set(key, value, duration, cb);
    }

    #[cfg(all(not(feature = "async"), not(feature = "sync")))]
    pub fn remove<F>(&self, key: &str, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.dispatch_remove(key, cb);
    }

    #[cfg(all(not(feature = "async"), not(feature = "sync")))]
    pub fn clear<F>(&self, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.dispatch_clear(cb);
    }

    #[cfg(all(not(feature = "async"), not(feature = "sync")))]
    pub fn cleanup<F>(&self, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        self.dispatch_cleanup(cb);
    }

    fn dispatch_get<F>(&self, key: &str, cb: F)
    where
        F: FnOnce(Result<Vec<u8>, Error>) + Send + Sync + 'static,
    {
        let msg = store::InputMessage::Get {
            path: self.0.path.clone(),
            key: key.into(),
            callback: Box::new(cb),
        };

        if let Err(e) = self.0.store_is.send(msg) {
            if let store::InputMessage::Get { callback, .. } = e.0 {
                callback(Err(Error::WorkerClosed));
            }
        }
    }

    fn dispatch_set<F>(&self, key: &str, value: &[u8], duration: Option<Duration>, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        let msg = store::InputMessage::Set {
            path: self.0.path.clone(),
            key: key.into(),
            value: value.into(),
            duration,
            callback: Box::new(cb),
        };

        if let Err(e) = self.0.store_is.send(msg) {
            if let store::InputMessage::Set { callback, .. } = e.0 {
                callback(Err(Error::WorkerClosed));
            }
        }
    }

    fn dispatch_remove<F>(&self, key: &str, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        let msg = store::InputMessage::Remove {
            path: self.0.path.clone(),
            key: key.into(),
            callback: Box::new(cb),
        };

        if let Err(e) = self.0.store_is.send(msg) {
            if let store::InputMessage::Remove { callback, .. } = e.0 {
                callback(Err(Error::WorkerClosed));
            }
        }
    }

    fn dispatch_clear<F>(&self, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        let msg = store::InputMessage::Clear {
            path: self.0.path.clone(),
            callback: Box::new(cb),
        };

        if let Err(e) = self.0.store_is.send(msg) {
            if let store::InputMessage::Clear { callback, .. } = e.0 {
                callback(Err(Error::WorkerClosed));
            }
        }
    }

    fn dispatch_cleanup<F>(&self, cb: F)
    where
        F: FnOnce(Result<(), Error>) + Send + Sync + 'static,
    {
        let msg = janitor::InputMessage::Cleanup(Box::new(cb));
        if let Err(e) = self.0.janitor_is.send(msg) {
            if let janitor::InputMessage::Cleanup(callback) = e.0 {
                callback(Err(Error::WorkerClosed));
            }
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let num_store_workers = self.store_handles.len();
        for _ in 0..num_store_workers {
            self.store_is.send(store::InputMessage::Quit).ok();
        }
        self.janitor_is.send(janitor::InputMessage::Quit).ok();

        for handle in self.store_handles.drain(..) {
            handle.join().ok();
        }

        if let Some(handle) = self.janitor_handle.take() {
            handle.join().ok();
        }
    }
}
