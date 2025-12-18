use anyhow::{Context, Result};
use rkyv::ser::writer::IoWriter;
use rkyv::util::AlignedVec;
use rkyv::Serialize;
use rkyv::{rancor, Portable};
use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::{any::TypeId, hash::Hash};

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MB buffer for file operations

pub(crate) trait Storage: AsRef<[u8]> {
    fn as_ptr(&self) -> *const u8 {
        self.as_ref().as_ptr()
    }
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

pub(crate) trait MutableStorage: Storage {
    fn new(size: usize) -> Result<Self>
    where
        Self: Sized;
    fn as_ref_mut(&mut self) -> &mut [u8];
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_ref_mut().as_mut_ptr()
    }
}

pub(crate) trait SharableStorage: Storage {
    fn get_id(&self) -> &str;
    fn from_id(os_id: &str) -> Result<Self>
    where
        Self: Sized;
}

pub(crate) trait LoadableStorage: Storage {
    fn load(path: &Path) -> Result<Self>
    where
        Self: Sized;
}

pub(crate) struct ArchiveStorage<T, S> {
    storage: S,
    phantom_t: PhantomData<T>,
}

// The `Send + Sync` trait is valid because the data is used read-only.
unsafe impl<T: Send, S> Send for ArchiveStorage<T, S> {}
unsafe impl<T: Sync, S> Sync for ArchiveStorage<T, S> {}

impl<T, S> ArchiveStorage<T, S>
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
    T::Archived: 'static + Portable,
    S: MutableStorage,
{
    pub(crate) fn new(data: T) -> Result<Self> {
        // We store an additional magic value at the beginning
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
        let mut storage =
            S::new(first_page.len() + bytes.len()).context("Failed to create storage")?;
        let ptr = storage.as_mut_ptr();
        unsafe {
            // Write the first page containing the magic value
            std::ptr::copy_nonoverlapping(first_page.as_ptr(), ptr, page_size::get());
            // Write the data after the magic value
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.add(page_size::get()), bytes.len());
        }
        Ok(Self {
            storage,
            phantom_t: PhantomData,
        })
    }
}

impl<T, S> AsRef<T::Archived> for ArchiveStorage<T, S>
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
    T: Sync + Send + 'static,
    T::Archived: Sync + Send + 'static + Portable,
    S: Storage + 'static,
{
    fn as_ref(&self) -> &T::Archived {
        unsafe {
            // Skip the first page because it contains the magic value
            let bytes = std::slice::from_raw_parts(
                self.storage.as_ptr().add(page_size::get()),
                self.storage.len() - page_size::get(),
            );
            rkyv::access_unchecked(bytes)
        }
    }
}

impl<T, S> ArchiveStorage<T, S>
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
    T::Archived: 'static + Portable,
    S: LoadableStorage,
{
    pub(crate) fn load(path: &Path) -> Result<Option<Self>> {
        let storage = S::load(path).context("Could not load storage!")?;

        // Make sure that file is large enough to contain magic and checksum
        let storage_len = storage.len();
        if storage_len < page_size::get() {
            eprintln!("Storage is too small to contain valid data");
            return Ok(None);
        }
        let magic_bytes_slice = &storage.as_ref()[..std::mem::size_of::<u64>()];
        let checksum_bytes_slice = &storage.as_ref()
            [std::mem::size_of::<u64>()..std::mem::size_of::<u64>() + std::mem::size_of::<u32>()];
        let data_bytes_slice = &storage.as_ref()[page_size::get()..storage.len()];

        // Read and verify the magic value
        let magic = u64::from_le_bytes(magic_bytes_slice.try_into().unwrap());
        let expected_magic = type_specific_magic::<T::Archived>();
        if magic != expected_magic {
            eprintln!("Invalid magic value in file");
            return Ok(None);
        }

        // Verify checksum
        let checksum_read = u32::from_le_bytes(checksum_bytes_slice.try_into().unwrap());
        let checksum_calculated = crc32fast::hash(data_bytes_slice);
        if checksum_read != checksum_calculated {
            eprintln!(
                "Checksum mismatch: expected {}, computed {}",
                checksum_read, checksum_calculated
            );
            return Ok(None);
        }

        Ok(Some(Self {
            storage,
            phantom_t: PhantomData,
        }))
    }
}

impl<T, S> ArchiveStorage<T, S>
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
    T::Archived: 'static + Portable,
    S: SharableStorage,
{
    pub(crate) fn get_id(&self) -> &str {
        self.storage.get_id()
    }

    pub(crate) fn from_id(id: &str) -> Result<Self> {
        // Map the shared memory using the OS ID
        let storage = S::from_id(id).context("Failed to open shared memory from ID")?;
        // Verify the magic value
        let magic_value = type_specific_magic::<T::Archived>();
        unsafe {
            let magic = std::ptr::read(storage.as_ptr() as *const u64);
            if magic != magic_value {
                anyhow::bail!("Invalid magic value in shared memory");
            }
        }
        Ok(Self {
            storage,
            phantom_t: PhantomData,
        })
    }
}

pub(crate) fn write_direct<T>(data: &T, path: &Path) -> Result<()>
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
    T::Archived: 'static + Portable,
{
    // Open file for writing and reading, truncating if it exists
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    // File layout: magic (u64) | checksum (u32) | remaining first page | data
    // We will write magic and data first, then compute and write checksum
    let seek_magic = SeekFrom::Start(0);
    let seek_checksum = SeekFrom::Start(std::mem::size_of::<u64>() as u64);
    let seek_data = SeekFrom::Start(page_size::get() as u64);

    // Calculate magic value and write it
    file.seek(seek_magic)?;
    file.write_all(&type_specific_magic::<T::Archived>().to_le_bytes())?;

    // Write main data with a buffered writer on top of file.
    // We drop the buffered writer immediately because it is not suitable for reading,
    // which we need later for checksum calculation.
    file.seek(seek_data)?;
    {
        let mut buf_writer = BufWriter::with_capacity(BUFFER_SIZE, &mut file);
        rkyv::api::high::to_bytes_in::<_, rancor::Error>(
            data,
            &mut IoWriter::new(&mut buf_writer),
        )?;
        buf_writer.flush()?;
    }

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

pub(crate) fn load_bytes<S: MutableStorage>(path: &Path) -> Result<S> {
    let size = std::fs::metadata(path)?.len() as usize;
    let mut storage = S::new(size)?;
    let mut file = File::open(path)?;
    let mut offset = 0;
    let mut buffer = vec![0u8; BUFFER_SIZE];
    loop {
        let bytes_read = file.read(buffer.as_mut_slice())?;
        if bytes_read == 0 {
            break;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr(),
                storage.as_mut_ptr().add(offset),
                bytes_read,
            );
        }
        offset += bytes_read;
    }
    Ok(storage)
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
    use shared_memory::ShmemConf;

    use super::*;
    use crate::storage::{MemoryStorage, MmapStorage, ShmemStorage};

    #[test]
    fn test_create() {
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        let container: ArchiveStorage<FastaMap, MemoryStorage> = ArchiveStorage::new(data).unwrap();
        let reference = container.as_ref();
        reference.names();
    }

    #[test]
    fn test_invalid_magic_shmem() {
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        let container: ArchiveStorage<FastaMap, ShmemStorage> = ArchiveStorage::new(data).unwrap();
        let os_id = container.get_id();
        let shmem = ShmemConf::new().os_id(os_id).open().unwrap();
        unsafe {
            let shmem_ptr = shmem.as_ptr();
            // Write an invalid magic value at the beginning of the shared memory
            std::ptr::write(shmem_ptr as *mut u64, 0);
        }
        let result: Result<ArchiveStorage<FastaMap, ShmemStorage>> = ArchiveStorage::from_id(os_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_os_id() {
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        let container: ArchiveStorage<FastaMap, ShmemStorage> = ArchiveStorage::new(data).unwrap();
        let os_id = container.get_id();
        let new_container: ArchiveStorage<FastaMap, ShmemStorage> =
            ArchiveStorage::from_id(os_id).unwrap();
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
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        let container: ArchiveStorage<FastaMap, MemoryStorage> =
            ArchiveStorage::new(data.clone()).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        write_direct(&data, temp_path).unwrap();
        // Load again
        let new_container: ArchiveStorage<FastaMap, MmapStorage> =
            ArchiveStorage::load(temp_path).unwrap().unwrap();
        assert_eq!(container.as_ref().names(), new_container.as_ref().names());
    }

    #[test]
    fn test_write_and_read_invalid_magic() {
        // Setup shmem fasta map
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        write_direct(&data, temp_path).unwrap();
        // Corrupt the magic value in the file
        let mut file = OpenOptions::new().write(true).open(temp_path).unwrap();
        file.write_all(&[0u8; 8]).unwrap();
        file.flush().unwrap();
        drop(file);
        // Attempt to read the shared memory archive back from the file
        let result: Option<ArchiveStorage<FastaMap, MmapStorage>> =
            ArchiveStorage::load(temp_path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_truncate_file_to_zero() {
        // Setup shmem fasta map
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        write_direct(&data, temp_path).unwrap();
        // Truncate the file to size 0
        let file = OpenOptions::new().write(true).open(temp_path).unwrap();
        file.set_len(0).unwrap();
        drop(file);
        // Attempt to read the shared memory archive back from the file
        let result: Option<ArchiveStorage<FastaMap, MmapStorage>> =
            ArchiveStorage::load(temp_path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_write_and_read_corrupted_data() {
        // Setup shmem fasta map
        let data = FastaMap::build("test-data/assemblies", true, 0, None, false, None).unwrap();
        // Write to a temporary file using write_to_file_direct
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();
        write_direct(&data, temp_path).unwrap();
        // Corrupt the data in the file (not the magic value or checksum)
        let data_offset = page_size::get();
        let file_len = std::fs::metadata(temp_path).unwrap().len() as usize;
        assert!(file_len > data_offset);
        let content_len = file_len - data_offset;
        let mut file = OpenOptions::new().write(true).open(temp_path).unwrap();
        file.seek(SeekFrom::Start(data_offset as u64)).unwrap();
        let corrupted = vec![0u8; content_len as usize];
        file.write_all(&corrupted).unwrap(); // Replace part of the data with zeros
        file.flush().unwrap();
        drop(file);
        // Attempt to read the shared memory archive back from the file
        let result: Option<ArchiveStorage<FastaMap, MmapStorage>> =
            ArchiveStorage::load(temp_path).unwrap();
        assert!(result.is_none());
    }
}
