use std::path::Path;

use crate::index::{FastaMap, TrackMap};
use crate::storage::{
    type_specific_magic, write_direct, ArchiveStorage, DynamicStorage, MemoryStorage, MmapStorage,
    ShmemStorage,
};
use anyhow::{anyhow, bail, Context, Result};
use rkyv::ser::writer::IoWriter;
use rkyv::util::AlignedVec;
use rkyv::Serialize;
use rkyv::{rancor, Portable};
use std::fs::File;
use std::io::BufWriter;

pub(crate) trait MapBuilder {
    fn build(
        dir: &str,
        strict: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
    ) -> Result<Self>
    where
        Self: Sized;
}

impl MapBuilder for FastaMap {
    fn build(
        dir: &str,
        strict: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
    ) -> Result<Self> {
        FastaMap::build(dir, strict, min_contig_length, num_workers, show_progress)
    }
}

impl MapBuilder for TrackMap {
    fn build(
        dir: &str,
        strict: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
    ) -> Result<Self> {
        TrackMap::build(dir, strict, min_contig_length, num_workers, show_progress)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn load<T>(
    dir: &str,
    cache_file_name: &str,
    strict: bool,
    min_contig_length: u64,
    num_workers: Option<usize>,
    show_progress: bool,
    storage_method: &str,
    no_cache: bool,
    force_build: bool,
) -> Result<DynamicStorage<T>>
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
    T: Sync + Send,
    T::Archived: 'static + Portable + Send + Sync,
    T: MapBuilder + 'static,
{
    if !strict && !no_cache {
        bail!("strict=false requires no_cache=true");
    }
    if no_cache && force_build {
        bail!("no_cache=true already implies force_build=true");
    }
    if no_cache && storage_method == "mmap" {
        bail!("storage_method=mmap requires no_cache=false");
    }
    let cache_path = Path::new(dir).join(format!(
        "{}-{:016x}",
        cache_file_name,
        type_specific_magic::<T>()
    ));
    if cache_path.exists() && !no_cache && !force_build {
        if storage_method == "memory" {
            match ArchiveStorage::<T, MemoryStorage>::load(&cache_path)
                .context(format!("Error reading cache {}", cache_path.display()))?
            {
                Some(archive) => {
                    return Ok(archive.into());
                }
                None => {
                    eprintln!("Cache file {} is corrupted.", cache_path.display());
                }
            }
        } else if storage_method == "shmem" {
            match ArchiveStorage::<T, ShmemStorage>::load(&cache_path)
                .context(format!("Error reading cache {}", cache_path.display()))?
            {
                Some(archive) => {
                    return Ok(archive.into());
                }
                None => {
                    eprintln!("Cache file {} is corrupted.", cache_path.display());
                }
            }
        } else if storage_method == "mmap" {
            match ArchiveStorage::<T, MmapStorage>::load(&cache_path)
                .context(format!("Error reading cache {}", cache_path.display()))?
            {
                Some(archive) => {
                    return Ok(archive.into());
                }
                None => {
                    eprintln!("Cache file {} is corrupted.", cache_path.display());
                }
            }
        } else {
            bail!("Unknown storage method: {}", storage_method);
        }
    }
    let map = T::build(dir, strict, min_contig_length, num_workers, show_progress)?;
    if no_cache {
        if storage_method == "memory" {
            let archive = ArchiveStorage::<T, MemoryStorage>::new(map)
                .context("Error creating memory storage archive")?;
            return Ok(archive.into());
        } else if storage_method == "shmem" {
            let archive = ArchiveStorage::<T, ShmemStorage>::new(map)
                .context("Error creating shmem storage archive")?;
            return Ok(archive.into());
        } else if storage_method == "mmap" {
            bail!("mmap storage requires no_cache=false");
        } else {
            bail!("Unknown storage method: {}", storage_method);
        }
    }
    eprintln!("Writing cache to {}", cache_path.display());
    write_direct(&map, &cache_path)?;
    std::mem::drop(map);
    if storage_method == "memory" {
        let archive = ArchiveStorage::<T, MemoryStorage>::load(&cache_path)?
            .ok_or(anyhow!("Newly written cache is corrupted!"))?;
        Ok(archive.into())
    } else if storage_method == "shmem" {
        let archive = ArchiveStorage::<T, ShmemStorage>::load(&cache_path)?
            .ok_or(anyhow!("Newly written cache is corrupted!"))?;
        Ok(archive.into())
    } else if storage_method == "mmap" {
        let archive = ArchiveStorage::<T, MmapStorage>::load(&cache_path)?
            .ok_or(anyhow!("Newly written cache is corrupted!"))?;
        Ok(archive.into())
    } else {
        bail!("Unknown storage method: {}", storage_method);
    }
}
