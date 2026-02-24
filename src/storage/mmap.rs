use std::{fs::File, path::PathBuf};

use crate::storage::{LoadableStorage, SharableStorage, Storage};
use anyhow::Result;
use memmap2::Mmap;

pub(crate) struct MmapStorage {
    path: String,
    mmap: Mmap,
}

impl AsRef<[u8]> for MmapStorage {
    fn as_ref(&self) -> &[u8] {
        &self.mmap
    }
}

impl Storage for MmapStorage {}

impl LoadableStorage for MmapStorage {
    fn load(path: &std::path::Path) -> Result<Self>
    where
        Self: Sized,
    {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(MmapStorage {
            path: path.to_string_lossy().into_owned(),
            mmap,
        })
    }
}

impl SharableStorage for MmapStorage {
    fn export(&self) -> Vec<u8> {
        self.path.clone().into_bytes()
    }

    fn import(data: Vec<u8>) -> Result<Self> {
        let path = String::from_utf8(data)?;
        Self::load(&PathBuf::from(path))
    }
}
