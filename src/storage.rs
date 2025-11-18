mod archive;
mod memory;
mod mmap;
mod shmem;

use std::fs::File;
use std::io::BufWriter;

use anyhow::Result;
use rkyv::ser::writer::IoWriter;
use rkyv::util::AlignedVec;
use rkyv::Serialize;
use rkyv::{rancor, Portable};
use std::convert::AsRef;

pub(crate) use archive::{
    type_specific_magic, write_direct, ArchiveStorage, LoadableStorage, SharableStorage, Storage,
};
pub(crate) use memory::MemoryStorage;
pub(crate) use mmap::MmapStorage;
pub(crate) use shmem::ShmemStorage;

pub(crate) enum DynamicStorage<T> {
    Memory(ArchiveStorage<T, MemoryStorage>),
    Shmem(ArchiveStorage<T, ShmemStorage>),
    Mmap(ArchiveStorage<T, MmapStorage>),
}

impl<T> DynamicStorage<T>
where
    // Trait bounds for rkyv serialization and deserialization, both to AlignedVec and IoWriter
    for<'a> T: Serialize<
        rancor::Strategy<
            rkyv::ser::Serializer<
                AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rancor::Error,
        >,
    >,
    for<'a, 'b, 'c, 'd> T: Serialize<
        rancor::Strategy<
            rkyv::ser::Serializer<
                &'b mut IoWriter<&'c mut BufWriter<&'d mut File>>,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rancor::Error,
        >,
    >,
    T::Archived: Sync + Send + 'static + Portable,
    T: Sync + Send + 'static,
{
    pub fn as_ref(&self) -> &T::Archived {
        match self {
            DynamicStorage::Memory(storage) => storage.as_ref(),
            DynamicStorage::Shmem(storage) => storage.as_ref(),
            DynamicStorage::Mmap(storage) => storage.as_ref(),
        }
    }

    pub fn get_id(&self) -> Option<String> {
        match self {
            DynamicStorage::Memory(_) => None,
            DynamicStorage::Shmem(storage) => Some(format!("Shmem:{}", storage.get_id())),
            DynamicStorage::Mmap(storage) => Some(format!("Mmap:{}", storage.get_id())),
        }
    }

    pub fn from_id(handle: &str) -> Result<DynamicStorage<T>> {
        let parts: Vec<&str> = handle.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid handle format");
        }
        let storage_type = parts[0];
        let id = parts[1];

        match storage_type {
            "Shmem" => {
                let storage = ArchiveStorage::<T, ShmemStorage>::from_id(id)?;
                Ok(DynamicStorage::Shmem(storage))
            }
            "Mmap" => {
                let storage = ArchiveStorage::<T, MmapStorage>::from_id(id)?;
                Ok(DynamicStorage::Mmap(storage))
            }
            _ => {
                anyhow::bail!("Unknown storage type: {}", storage_type);
            }
        }
    }
}

impl<T> From<ArchiveStorage<T, MemoryStorage>> for DynamicStorage<T> {
    fn from(storage: ArchiveStorage<T, MemoryStorage>) -> Self {
        DynamicStorage::Memory(storage)
    }
}

impl<T> From<ArchiveStorage<T, ShmemStorage>> for DynamicStorage<T> {
    fn from(storage: ArchiveStorage<T, ShmemStorage>) -> Self {
        DynamicStorage::Shmem(storage)
    }
}

impl<T> From<ArchiveStorage<T, MmapStorage>> for DynamicStorage<T> {
    fn from(storage: ArchiveStorage<T, MmapStorage>) -> Self {
        DynamicStorage::Mmap(storage)
    }
}
