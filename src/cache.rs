use std::fs::File;
use std::path::Path;

use crate::index::{ArchivedFastaMap, ArchivedTrackMap, FastaMap, TrackMap};
use crate::shmem::{type_specific_magic, ShmemArchive};
use crate::util::get_relative_name_without_suffix;
use anyhow::{bail, Result};

pub(super) fn load_fasta_map(
    dir: &str,
    strict: bool,
    force_build: bool,
    no_cache: bool,
    min_contig_length: u64,
    num_workers: Option<usize>,
    show_progress: bool,
) -> Result<ShmemArchive<FastaMap>> {
    if !strict && !no_cache {
        bail!("strict=false requires no_cache=true");
    }
    if no_cache && force_build {
        bail!("no_cache=true already implies force_build=true");
    }
    let magic_value = type_specific_magic::<ArchivedFastaMap>();
    let cache_path = Path::new(dir).join(format!(".fasta-map-cache-{:016x}", magic_value));
    if cache_path.exists() && !no_cache && !force_build {
        let expected_names = get_expected_names(dir, ".fna.gz")?;
        match ShmemArchive::read_from_file(&File::open(&cache_path)?) {
            Ok(archive) => {
                let archive_names = (archive.as_ref() as &ArchivedFastaMap).names();
                if expected_names != archive_names {
                    eprintln!("Cache names do not match expected names. Rebuilding cache.");
                } else {
                    return Ok(archive);
                }
            }
            Err(e) => {
                eprintln!(
                    "Error loading cache: {}. Rebuilding. Error: {:?}",
                    cache_path.display(),
                    e
                );
            }
        }
    }
    let fasta_map = FastaMap::build(dir, strict, min_contig_length, num_workers, show_progress)?;
    if no_cache {
        return ShmemArchive::new(fasta_map);
    }
    ShmemArchive::write_to_file_direct(&fasta_map, &cache_path)?;
    std::mem::drop(fasta_map);
    let archive = ShmemArchive::read_from_file(&File::open(cache_path)?)?;
    Ok(archive)
}

pub(super) fn load_track_map(
    dir: &str,
    strict: bool,
    force_build: bool,
    no_cache: bool,
    min_contig_length: u64,
    num_workers: Option<usize>,
    show_progress: bool,
) -> Result<ShmemArchive<TrackMap>> {
    if !strict && !no_cache {
        bail!("strict=false requires no_cache=true");
    }
    if no_cache && force_build {
        bail!("no_cache=true already implies force_build=true");
    }
    let magic_value = type_specific_magic::<ArchivedTrackMap>();
    let cache_path = Path::new(dir).join(format!(".track-map-cache-{:016x}", magic_value));
    if cache_path.exists() && !no_cache && !force_build {
        let expected_names = get_expected_names(dir, ".track.gz")?;
        match ShmemArchive::read_from_file(&File::open(&cache_path)?) {
            Ok(archive) => {
                let archive_names = (archive.as_ref() as &ArchivedTrackMap).names();
                if expected_names != archive_names {
                    eprintln!("Cache names do not match expected names. Rebuilding cache.");
                } else {
                    return Ok(archive);
                }
            }
            Err(e) => {
                eprintln!(
                    "Error loading cache: {}. Rebuilding. Error: {:?}",
                    cache_path.display(),
                    e
                );
            }
        }
    }
    let track_map = TrackMap::build(dir, strict, min_contig_length, num_workers, show_progress)?;
    if no_cache {
        return ShmemArchive::new(track_map);
    }
    ShmemArchive::write_to_file_direct(&track_map, &cache_path)?;
    std::mem::drop(track_map);
    let archive = ShmemArchive::read_from_file(&File::open(cache_path)?)?;
    Ok(archive)
}

fn get_expected_names(dir: &str, suffix: &str) -> Result<Vec<String>> {
    let root_path = Path::new(dir);
    // Get sorted list of files
    let mut files = glob::glob(format!("{dir}/**/*{suffix}").as_str())?
        .map(|entry| {
            let path = entry?;
            let name = get_relative_name_without_suffix(&path, root_path, suffix)?;
            Ok(name.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    files.sort();
    Ok(files)
}
