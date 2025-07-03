use anyhow::Result;
use rkyv::util::AlignedVec;
use rkyv::Serialize;
use rkyv::{rancor, Portable};
use shared_memory::{Shmem, ShmemConf};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::Hasher;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::{any::TypeId, hash::Hash};

pub(crate) struct ShmemArchive<T> {
    shmem: Shmem,
    phantom_t: PhantomData<T>,
}

// The `Send + Sync` trait is valid because the data is used read-only.
unsafe impl<T: Send> Send for ShmemArchive<T> {}
unsafe impl<T: Send> Sync for ShmemArchive<T> {}

impl<T> ShmemArchive<T>
where
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
    T::Archived: 'static + Portable,
{
    pub(crate) fn new(data: &T) -> Result<Self> {
        // We store an additional magic value at the beginning of the shared memory
        // to verify the data type during access.
        let magic_value = type_specific_magic::<T::Archived>();
        // For alignment, we just store the magic value in the first page
        // and the actual data in the following pages.
        let mut first_page = Vec::with_capacity(page_size::get());
        first_page.extend_from_slice(magic_value.to_ne_bytes().as_slice());
        first_page.resize(page_size::get(), 0);
        // Serialize the data to bytes (copy)
        let bytes = rkyv::to_bytes::<rancor::Error>(data)?;
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
        Ok(Self {
            shmem,
            phantom_t: PhantomData,
        })
    }

    pub(crate) fn as_ref(&self) -> &T::Archived {
        unsafe {
            // Skip the first page because it contains the magic value
            let bytes = std::slice::from_raw_parts(
                self.shmem.as_ptr().add(page_size::get()),
                self.shmem.len() - page_size::get(),
            );
            rkyv::access_unchecked(bytes)
        }
    }

    pub(crate) fn get_os_id(&self) -> &str {
        self.shmem.get_os_id()
    }

    pub(crate) fn from_os_id(os_id: &str) -> Result<Self> {
        // Map the shared memory using the OS ID
        let shmem = ShmemConf::new().os_id(os_id).open()?;
        let shmem_ptr = shmem.as_ptr();
        // Verify the magic value
        let magic_value = type_specific_magic::<T::Archived>();
        unsafe {
            let magic = std::ptr::read(shmem_ptr as *const u64);
            if magic != magic_value {
                anyhow::bail!("Invalid magic value in shared memory");
            }
        }
        Ok(Self {
            shmem,
            phantom_t: PhantomData,
        })
    }

    pub(crate) fn write_to_file(&self, file: &File) -> Result<()> {
        // Transmute to byte slice and compute checksum
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self.shmem.as_ptr().add(page_size::get()),
                self.shmem.len() - page_size::get(),
            )
        };
        let checksum = crc32fast::hash(bytes);
        // Write
        let mut writer = std::io::BufWriter::new(file);
        let magic_value = type_specific_magic::<T::Archived>();
        writer.write_all(&magic_value.to_le_bytes())?;
        writer.write_all(&checksum.to_le_bytes())?;
        writer.write_all(bytes)?;
        Ok(())
    }

    pub(crate) fn read_from_file(file: &File) -> Result<Self> {
        let mut reader = std::io::BufReader::new(file);

        // Read and verify the magic value
        let mut magic_bytes = vec![0u8; std::mem::size_of::<u64>()];
        reader.read_exact(&mut magic_bytes)?;
        let magic = u64::from_le_bytes(magic_bytes.try_into().unwrap());
        let expected_magic = type_specific_magic::<T::Archived>();
        if magic != expected_magic {
            anyhow::bail!("Invalid magic value in file");
        }

        // Read the checksum
        let mut checksum_bytes = vec![0u8; std::mem::size_of::<u32>()];
        reader.read_exact(&mut checksum_bytes)?;
        let checksum_read = u32::from_le_bytes(checksum_bytes.try_into().unwrap());

        // Create shared memory
        let file_len = file.metadata()?.len();
        let header_len = (std::mem::size_of::<u64>() + std::mem::size_of::<u32>()) as u64;
        if file_len < header_len {
            anyhow::bail!("File is too small to contain valid data");
        }
        let data_len = file_len - header_len;
        let shmem = ShmemConf::new()
            .size(page_size::get() + data_len as usize)
            .create()?;
        let shmem_ptr = shmem.as_ptr();
        unsafe {
            let magic_bytes = magic.to_ne_bytes();
            std::ptr::copy_nonoverlapping(
                magic_bytes.as_ptr(),
                shmem_ptr,
                std::mem::size_of::<u64>(),
            );
        }

        // Read data to shared memory
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                shmem_ptr.add(page_size::get()),
                shmem.len() - page_size::get(),
            )
        };
        reader.read_exact(bytes)?;

        // Verify checksum
        let checksum_calculated = crc32fast::hash(bytes);
        if checksum_read != checksum_calculated {
            anyhow::bail!(
                "Checksum mismatch: expected {}, computed {}",
                checksum_read,
                checksum_calculated
            );
        }

        Ok(Self {
            shmem,
            phantom_t: PhantomData,
        })
    }
}

fn type_specific_magic<T: 'static>() -> u64 {
    let mut hasher = DefaultHasher::new();
    TypeId::of::<T>().hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::io::Seek;
    use std::io::Write;
    use tempfile::tempfile;

    use crate::index::FastaMap;

    use super::*;

    #[test]
    fn test_create() {
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        let reference = container.as_ref();
        reference.names();
    }

    #[test]
    fn test_invalid_magic() {
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        let os_id = container.get_os_id();
        let shmem = ShmemConf::new().os_id(os_id).open().unwrap();
        unsafe {
            let shmem_ptr = shmem.as_ptr();
            // Write an invalid magic value at the beginning of the shared memory
            std::ptr::write(shmem_ptr as *mut u64, 0);
        }
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::from_os_id(os_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_os_id() {
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        let os_id = container.get_os_id();
        let new_container: ShmemArchive<FastaMap> = ShmemArchive::from_os_id(os_id).unwrap();
        assert_eq!(container.as_ref().names(), new_container.as_ref().names());
    }

    #[test]
    fn test_nontrivial_magic() {
        let magic_value = type_specific_magic::<FastaMap>();
        println!("Magic value: {:#x}", magic_value);
        assert_ne!(magic_value, 0);
        assert_ne!(magic_value, 1);
    }

    #[test]
    fn test_write_and_read_from_file() {
        // Setup shmem fasta map
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        // Write to a temporary file
        let mut temp_file = tempfile().unwrap();
        container.write_to_file(&temp_file).unwrap();
        // Flush and rewind to beginning
        temp_file.flush().unwrap();
        temp_file.rewind().unwrap();
        // Read the shared memory archive back from the file
        let new_container: ShmemArchive<FastaMap> =
            ShmemArchive::read_from_file(&temp_file).unwrap();
        assert_eq!(container.as_ref().names(), new_container.as_ref().names());
    }

    #[test]
    fn test_write_and_read_invalid_magic() {
        // Setup shmem fasta map
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        // Write to a temporary file
        let mut temp_file = tempfile().unwrap();
        container.write_to_file(&temp_file).unwrap();
        // Flush and rewind to beginning
        temp_file.flush().unwrap();
        temp_file.rewind().unwrap();
        // Corrupt the magic value in the file
        temp_file.write_all(&[0u8; 8]).unwrap();
        temp_file.flush().unwrap();
        temp_file.rewind().unwrap();
        // Attempt to read the shared memory archive back from the file
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::read_from_file(&temp_file);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncate_file_to_zero() {
        // Setup shmem fasta map
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        // Write to a temporary file
        let mut temp_file = tempfile().unwrap();
        container.write_to_file(&temp_file).unwrap();
        // Truncate the file to size 0
        temp_file.flush().unwrap();
        temp_file.set_len(0).unwrap();
        temp_file.rewind().unwrap();
        // Attempt to read the shared memory archive back from the file
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::read_from_file(&temp_file);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_and_read_corrupted_data() {
        // Setup shmem fasta map
        let data = FastaMap::build("test_data", true, 0).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(&data).unwrap();
        // Write to a temporary file
        let mut temp_file = tempfile().unwrap();
        container.write_to_file(&temp_file).unwrap();
        // Flush and rewind to beginning
        temp_file.flush().unwrap();
        temp_file.rewind().unwrap();
        // Corrupt the data in the file (not the magic value or checksum)
        let header_len = (std::mem::size_of::<u64>() + std::mem::size_of::<u32>()) as u64;
        let file_len = temp_file.metadata().unwrap().len();
        assert!(file_len > header_len);
        let content_len = file_len - header_len;
        temp_file
            .seek(std::io::SeekFrom::Start(header_len))
            .unwrap();
        let corrupted = vec![0u8; content_len as usize];
        temp_file.write_all(&corrupted).unwrap(); // Replace part of the data with zeros
        temp_file.flush().unwrap();
        temp_file.rewind().unwrap();
        // Attempt to read the shared memory archive back from the file
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::read_from_file(&temp_file);
        assert!(result.is_err());
    }
}
