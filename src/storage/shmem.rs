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
    fn export(&self) -> Vec<u8> {
        self.shmem.get_os_id().as_bytes().to_vec()
    }

    fn import(data: Vec<u8>) -> Result<Self>
    where
        Self: Sized,
    {
        let os_id_str = String::from_utf8(data)?;
        let shmem = ShmemConf::new().os_id(os_id_str).open()?;
        Ok(ShmemStorage { shmem })
    }
}
