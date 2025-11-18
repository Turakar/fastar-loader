use crate::storage::archive::{load_bytes, LoadableStorage, MutableStorage, Storage};
use anyhow::Result;

pub(crate) struct MemoryStorage {
    data: Vec<u8>,
}

impl AsRef<[u8]> for MemoryStorage {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Storage for MemoryStorage {}

impl MutableStorage for MemoryStorage {
    fn new(size: usize) -> anyhow::Result<Self> {
        Ok(MemoryStorage {
            data: vec![0u8; size],
        })
    }

    fn as_ref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl LoadableStorage for MemoryStorage {
    fn load(path: &std::path::Path) -> Result<Self> {
        load_bytes(path)
    }
}
