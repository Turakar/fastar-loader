use anyhow::Result;
use shared_memory::{Shmem, ShmemConf};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::{any::TypeId, hash::Hash};

use crate::index::{ArchivedIndexMap, IndexMap};

pub struct ShmemIndexMap {
    shmem: Shmem,
}

// The `Send + Sync` trait is valid because the data is used read-only.
unsafe impl Send for ShmemIndexMap {}
unsafe impl Sync for ShmemIndexMap {}

impl ShmemIndexMap {
    pub fn new(data: &IndexMap) -> Result<Self> {
        // We store an additional magic value at the beginning of the shared memory
        // to verify the data type during access.
        let magic_value = type_specific_magic::<ArchivedIndexMap>();
        // For alignment, we just store the magic value in the first page
        // and the actual data in the following pages.
        let mut first_page = Vec::with_capacity(page_size::get());
        first_page.extend_from_slice(magic_value.to_ne_bytes().as_slice());
        first_page.resize(page_size::get(), 0);
        // Serialize the data to bytes (copy)
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(data)?;
        // Allocate shared memory
        let shmem = ShmemConf::new()
            .size(first_page.len() + bytes.len())
            .create()?;
        let shmem_ptr = shmem.as_ptr();
        unsafe {
            // Write the first page containing the magic value
            std::ptr::copy_nonoverlapping(first_page.as_ptr(), shmem_ptr, page_size::get());
            // Write the data after the magic value
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                shmem_ptr.add(page_size::get()),
                bytes.len(),
            );
        }
        Ok(Self { shmem })
    }

    pub fn as_ref(&self) -> &ArchivedIndexMap {
        unsafe {
            // Skip the first page because it contains the magic value
            let bytes = std::slice::from_raw_parts(
                self.shmem.as_ptr().add(page_size::get()),
                self.shmem.len() - page_size::get(),
            );
            rkyv::access_unchecked(bytes)
        }
    }

    pub fn get_os_id(&self) -> &str {
        self.shmem.get_os_id()
    }

    pub fn from_os_id(os_id: &str) -> Result<Self> {
        // Map the shared memory using the OS ID
        let shmem = ShmemConf::new().os_id(os_id).open()?;
        let shmem_ptr = shmem.as_ptr();
        // Verify the magic value
        let magic_value = type_specific_magic::<ArchivedIndexMap>();
        unsafe {
            let magic = std::ptr::read(shmem_ptr as *const u64);
            if magic != magic_value {
                anyhow::bail!("Invalid magic value in shared memory");
            }
        }
        Ok(Self { shmem })
    }
}

fn type_specific_magic<T: 'static>() -> u64 {
    let mut hasher = DefaultHasher::new();
    TypeId::of::<T>().hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use crate::index::IndexMapTrait;

    use super::*;

    #[test]
    fn test_create() {
        let data = IndexMap::build("test_data").unwrap();
        let container = ShmemIndexMap::new(&data).unwrap();
        let reference = container.as_ref();
        assert_eq!(reference.names(), data.names());
    }

    #[test]
    fn test_invalid_magic() {
        let data = IndexMap::build("test_data").unwrap();
        let container = ShmemIndexMap::new(&data).unwrap();
        let os_id = container.get_os_id();
        let shmem = ShmemConf::new().os_id(os_id).open().unwrap();
        unsafe {
            let shmem_ptr = shmem.as_ptr();
            // Write an invalid magic value at the beginning of the shared memory
            std::ptr::write(shmem_ptr as *mut u64, 0);
        }
        let result = ShmemIndexMap::from_os_id(os_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_os_id() {
        let data = IndexMap::build("test_data").unwrap();
        let container = ShmemIndexMap::new(&data).unwrap();
        let os_id = container.get_os_id();
        let new_container = ShmemIndexMap::from_os_id(os_id).unwrap();
        assert_eq!(container.as_ref().names(), new_container.as_ref().names());
    }

    #[test]
    fn test_nontrivial_magic() {
        let magic_value = type_specific_magic::<ArchivedIndexMap>();
        println!("Magic value: {:#x}", magic_value);
        assert_ne!(magic_value, 0);
        assert_ne!(magic_value, 1);
    }
}
