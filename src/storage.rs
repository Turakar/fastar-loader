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

    pub fn export(&self) -> Option<Vec<u8>> {
        fn prefix(storage_type: &str, id: Vec<u8>) -> Vec<u8> {
            let mut result = storage_type.as_bytes().to_vec();
            result.push(b':');
            result.extend(id);
            result
        }

        match self {
            DynamicStorage::Memory(storage) => Some(prefix("Memory", storage.export())),
            DynamicStorage::Shmem(storage) => Some(prefix("Shmem", storage.export())),
            DynamicStorage::Mmap(storage) => Some(prefix("Mmap", storage.export())),
        }
    }

    pub fn import(mut data: Vec<u8>) -> Result<DynamicStorage<T>> {
        let colon = data
            .iter()
            .position(|&b| b == b':')
            .ok_or_else(|| anyhow::anyhow!("Invalid handle format: missing colon separator"))?;
        let storage_type = std::str::from_utf8(&data[..colon])?.to_string();
        let id = data.split_off(colon + 1);

        match storage_type.as_str() {
            "Memory" => {
                let storage = ArchiveStorage::<T, MemoryStorage>::import(id)?;
                Ok(DynamicStorage::Memory(storage))
            }
            "Shmem" => {
                let storage = ArchiveStorage::<T, ShmemStorage>::import(id)?;
                Ok(DynamicStorage::Shmem(storage))
            }
            "Mmap" => {
                let storage = ArchiveStorage::<T, MmapStorage>::import(id)?;
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
