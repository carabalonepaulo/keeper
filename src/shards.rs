use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

#[derive(Debug, Clone)]
pub struct Shards(Arc<[RwLock<()>; 4096]>);

impl Shards {
    pub fn new() -> Self {
        Self(Arc::new(std::array::from_fn(|_| RwLock::new(()))))
    }

    pub fn read(&self, id: u16) -> RwLockReadGuard<'_, ()> {
        self.0[id as usize].read().expect("lock poisoned")
    }

    pub fn write(&self, id: u16) -> RwLockWriteGuard<'_, ()> {
        self.0[id as usize].write().expect("lock poisoned")
    }

    pub fn try_read(
        &self,
        id: u16,
    ) -> Result<RwLockReadGuard<'_, ()>, TryLockError<RwLockReadGuard<'_, ()>>> {
        self.0[id as usize].try_read()
    }

    pub fn try_write(
        &self,
        id: u16,
    ) -> Result<RwLockWriteGuard<'_, ()>, TryLockError<RwLockWriteGuard<'_, ()>>> {
        self.0[id as usize].try_write()
    }
}
