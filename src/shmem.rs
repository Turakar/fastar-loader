use anyhow::Result;
use rkyv::ser::writer::IoWriter;
use rkyv::util::AlignedVec;
use rkyv::Serialize;
use rkyv::{rancor, Portable};
use shared_memory::{Shmem, ShmemConf};
use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::{any::TypeId, hash::Hash};

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MB buffer for file operations

pub(crate) struct ShmemArchive<T> {
    shmem: Shmem,
    phantom_t: PhantomData<T>,
}

// The `Send + Sync` trait is valid because the data is used read-only.
unsafe impl<T: Send> Send for ShmemArchive<T> {}
unsafe impl<T: Send> Sync for ShmemArchive<T> {}

impl<T> ShmemArchive<T>
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
    for<'a, 'b> T: Serialize<
        rancor::Strategy<
            rkyv::ser::Serializer<
                &'b mut IoWriter<&'b mut File>,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rancor::Error,
        >,
    >,
    T::Archived: 'static + Portable,
{
    pub(crate) fn new(data: T) -> Result<Self> {
        // We store an additional magic value at the beginning of the shared memory
        // to verify the data type during access.
        let magic_value = type_specific_magic::<T::Archived>();
        // For alignment, we just store the magic value in the first page
        // and the actual data in the following pages.
        let mut first_page = Vec::with_capacity(page_size::get());
        first_page.extend_from_slice(magic_value.to_ne_bytes().as_slice());
        first_page.resize(page_size::get(), 0);
        // Serialize the data to bytes (copy), then forget the original data
        let bytes = rkyv::to_bytes::<rancor::Error>(&data)?;
        std::mem::drop(data);
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

    pub(crate) fn write_to_file_direct(data: &T, path: &Path) -> Result<()> {
        // Open file for writing and reading, truncating if it exists
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // File layout: magic (u64) | checksum (u32) | data
        // We will write magic and data first, then compute and write checksum
        let seek_magic = SeekFrom::Start(0);
        let seek_checksum = SeekFrom::Start(std::mem::size_of::<u64>() as u64);
        let seek_data =
            SeekFrom::Start((std::mem::size_of::<u64>() + std::mem::size_of::<u32>()) as u64);

        // Calculate magic value and write it
        file.seek(seek_magic)?;
        file.write_all(&type_specific_magic::<T::Archived>().to_le_bytes())?;

        // Write main data
        file.seek(seek_data)?;
        let mut writer = IoWriter::new(&mut file);
        rkyv::api::high::to_bytes_in::<_, rancor::Error>(data, &mut writer)?;

        // Calculate checksum of main data and write it
        file.seek(seek_data)?;
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut hasher = crc32fast::Hasher::new();
        loop {
            let bytes_read = file.read(buffer.as_mut_slice())?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        let checksum = hasher.finalize();
        file.seek(seek_checksum)?;
        file.write_all(&checksum.to_le_bytes())?;
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

pub(crate) fn type_specific_magic<T: 'static>() -> u64 {
    let mut hasher = DefaultHasher::new();
    TypeId::of::<T>().hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::index::FastaMap;

    use super::*;

    #[test]
    fn test_create() {
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(data).unwrap();
        let reference = container.as_ref();
        reference.names();
    }

    #[test]
    fn test_invalid_magic() {
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(data).unwrap();
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
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(data).unwrap();
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
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        let container: ShmemArchive<FastaMap> = ShmemArchive::new(data.clone()).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        ShmemArchive::write_to_file_direct(&data, temp_path).unwrap();
        // Reopen file for reading
        let file = File::open(temp_path).unwrap();
        let new_container: ShmemArchive<FastaMap> = ShmemArchive::read_from_file(&file).unwrap();
        assert_eq!(container.as_ref().names(), new_container.as_ref().names());
    }

    #[test]
    fn test_write_and_read_invalid_magic() {
        // Setup shmem fasta map
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        ShmemArchive::write_to_file_direct(&data, temp_path).unwrap();
        // Corrupt the magic value in the file
        let mut file = OpenOptions::new().write(true).open(temp_path).unwrap();
        file.write_all(&[0u8; 8]).unwrap();
        file.flush().unwrap();
        drop(file);
        // Attempt to read the shared memory archive back from the file
        let file = File::open(temp_path).unwrap();
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::read_from_file(&file);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncate_file_to_zero() {
        // Setup shmem fasta map
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        ShmemArchive::write_to_file_direct(&data, temp_path).unwrap();
        // Truncate the file to size 0
        let file = OpenOptions::new().write(true).open(temp_path).unwrap();
        file.set_len(0).unwrap();
        drop(file);
        // Attempt to read the shared memory archive back from the file
        let file = File::open(temp_path).unwrap();
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::read_from_file(&file);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_and_read_corrupted_data() {
        // Setup shmem fasta map
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        ShmemArchive::write_to_file_direct(&data, temp_path).unwrap();
        // Corrupt the data in the file (not the magic value or checksum)
        let header_len = (std::mem::size_of::<u64>() + std::mem::size_of::<u32>()) as u64;
        let file_len = std::fs::metadata(temp_path).unwrap().len();
        assert!(file_len > header_len);
        let content_len = file_len - header_len;
        let mut file = OpenOptions::new().write(true).open(temp_path).unwrap();
        file.seek(SeekFrom::Start(header_len)).unwrap();
        let corrupted = vec![0u8; content_len as usize];
        file.write_all(&corrupted).unwrap(); // Replace part of the data with zeros
        file.flush().unwrap();
        drop(file);
        // Attempt to read the shared memory archive back from the file
        let file = File::open(temp_path).unwrap();
        let result: Result<ShmemArchive<FastaMap>> = ShmemArchive::read_from_file(&file);
        assert!(result.is_err());
    }
}
