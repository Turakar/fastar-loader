use crate::storage::archive::{
    load_bytes, LoadableStorage, MutableStorage, SharableStorage, Storage,
};
use anyhow::Result;
use shared_memory::{Shmem, ShmemConf};

pub(crate) struct ShmemStorage {
    shmem: Shmem,
}

impl AsRef<[u8]> for ShmemStorage {
    fn as_ref(&self) -> &[u8] {
        unsafe { self.shmem.as_slice() }
    }
}

impl Storage for ShmemStorage {}

impl MutableStorage for ShmemStorage {
    fn new(size: usize) -> anyhow::Result<Self> {
        let shmem = ShmemConf::new().size(size).create()?;
        Ok(ShmemStorage { shmem })
    }

    fn as_ref_mut(&mut self) -> &mut [u8] {
        unsafe { self.shmem.as_slice_mut() }
    }
}

impl LoadableStorage for ShmemStorage {
    fn load(path: &std::path::Path) -> Result<Self> {
        load_bytes(path)
    }
}

impl SharableStorage for ShmemStorage {
    fn get_id(&self) -> &str {
        self.shmem.get_os_id()
    }

    fn from_id(os_id: &str) -> Result<Self>
    where
        Self: Sized,
    {
        let shmem = ShmemConf::new().os_id(os_id).open()?;
        Ok(ShmemStorage { shmem })
    }
}
