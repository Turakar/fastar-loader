use sha2::{Digest, Sha256};
use std::fs::File;
use std::path::Path;

use crate::index::{FastaMap, TrackMap};
use crate::shmem::ShmemArchive;
use crate::util::get_name_without_suffix;
use anyhow::Result;

pub fn load_fasta_map(
    dir: &str,
    strict: bool,
    force_build: bool,
    no_cache: bool,
) -> Result<ShmemArchive<FastaMap>> {
    if no_cache {
        let fasta_map = FastaMap::build(dir, strict)?;
        let archive = ShmemArchive::new(&fasta_map)?;
        return Ok(archive);
    }
    let cache_path = Path::new(dir).join(format!(
        ".fasta-map-cache-{}",
        hash_dir(dir, strict, ".track.gz")?
    ));
    if cache_path.exists() && !force_build {
        match ShmemArchive::read_from_file(&File::open(&cache_path)?) {
            Ok(archive) => return Ok(archive),
            Err(e) => {
                eprintln!(
                    "Error loading cache: {}. Rebuilding. Error: {:?}",
                    cache_path.display(),
                    e
                );
            }
        }
    }
    let fasta_map = FastaMap::build(dir, strict)?;
    let archive = ShmemArchive::new(&fasta_map)?;
    archive.write_to_file(&File::create(&cache_path)?)?;
    Ok(archive)
}

pub fn load_track_map(
    dir: &str,
    strict: bool,
    force_build: bool,
    no_cache: bool,
) -> Result<ShmemArchive<TrackMap>> {
    if no_cache {
        let track_map = TrackMap::build(dir, strict)?;
        let archive = ShmemArchive::new(&track_map)?;
        return Ok(archive);
    }
    let cache_path = Path::new(dir).join(format!(
        ".track-map-cache-{}",
        hash_dir(dir, strict, ".fna.gz")?
    ));
    if cache_path.exists() && !force_build {
        match ShmemArchive::read_from_file(&File::open(&cache_path)?) {
            Ok(archive) => return Ok(archive),
            Err(e) => {
                eprintln!(
                    "Error loading cache: {}. Rebuilding. Error: {:?}",
                    cache_path.display(),
                    e
                );
            }
        }
    }
    let track_map = TrackMap::build(dir, strict)?;
    let archive = ShmemArchive::new(&track_map)?;
    archive.write_to_file(&File::create(&cache_path)?)?;
    Ok(archive)
}

fn hash_dir(dir: &str, strict: bool, suffix: &str) -> Result<String> {
    // Get sorted list of files
    let mut files = glob::glob(format!("{dir}/*{suffix}").as_str())?
        .map(|entry| {
            let path = entry?;
            let name = get_name_without_suffix(&path, suffix)?;
            Ok(name.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    files.sort();
    let mut hasher = Sha256::new();
    hasher.update(u8::from(strict).to_le_bytes());
    files.into_iter().for_each(|file_name| {
        hasher.update(file_name.as_bytes());
    });
    Ok(format!("{:16x}", hasher.finalize()))
}
