use crate::index::bgzf_index::BgzfIndex;
use crate::util::get_name_without_suffix;
use anyhow::Context;
use noodles::bgzf::{self, io::Seek, VirtualPosition};

use anyhow::Result;
use numpy::ndarray::Array1;
use rayon::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};
use std::io::Read;
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
};

use crate::util::with_suffix;

use super::track_index::TrackIndex;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Index {
    gzi: BgzfIndex,
    track_index: TrackIndex,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct TrackMap {
    dir: String,
    map: BTreeMap<String, Index>,
}

impl TrackMap {
    pub(crate) fn build(
        dir: &str,
        strict: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
    ) -> Result<Self> {
        let paths = glob::glob(format!("{}/*.track.gz", dir).as_str())?
            .map(|entry| entry.map_err(anyhow::Error::from))
            .collect::<Result<Vec<_>>>()?;
        let num_paths = paths.len();

        // Build indices in parallel using rayon. If num_workers is set, use a custom thread pool.
        let build_indices = || {
            let results: Result<Vec<Option<(String, Index)>>, anyhow::Error> = paths
                .par_iter()
                .enumerate()
                .map(|(i, track_path)| {
                    if i % 100 == 0 && num_paths > 100 {
                        eprintln!("Processed {}/{} track indices", i, num_paths);
                    }
                    match Self::index_path(track_path, min_contig_length) {
                        Ok((track_name, index)) => Ok(Some((track_name, index))),
                        Err(e) => {
                            if strict {
                                Err(e)
                            } else {
                                eprintln!(
                                    "Error processing track: {}. Skipping. Error: {:?}",
                                    track_path.display(),
                                    e
                                );
                                Ok(None)
                            }
                        }
                    }
                })
                .collect();
            results
        };

        let results = if let Some(workers) = num_workers {
            // Use a custom thread pool for this operation only
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            pool.install(build_indices)?
        } else {
            build_indices()?
        };

        // Convert Vec<Option<(String, Index)>> to BTreeMap
        let map = results.into_iter().flatten().collect::<BTreeMap<_, _>>();
        eprintln!("Processed {} track indices", num_paths);
        Ok(TrackMap {
            map,
            dir: dir.to_string(),
        })
    }

    fn index_path(track_path: &Path, min_contig_length: u64) -> Result<(String, Index)> {
        let track_name = get_name_without_suffix(track_path, ".track.gz")?;
        let gzi = BgzfIndex::read(with_suffix(track_path.to_path_buf(), ".gzi"))
            .context("Failed to read .gzi")?;
        let track_index = TrackIndex::read(
            with_suffix(track_path.to_path_buf(), ".idx"),
            min_contig_length,
        )
        .context("Failed to read .idx")?;
        Ok((track_name, Index { gzi, track_index }))
    }
}

impl ArchivedTrackMap {
    pub(crate) fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    pub(crate) fn contigs(&self, track_name: &str) -> Result<Vec<(&[u8], u64)>> {
        let entry = self
            .map
            .get(track_name)
            .ok_or(anyhow::anyhow!("Track name not found"))?;
        Ok(entry.track_index.contigs())
    }

    pub(crate) fn query(
        &self,
        track_name: &str,
        contig: &[u8],
        start: u64,
    ) -> Result<(PathBuf, VirtualPosition)> {
        // Search in index
        let entry = self
            .map
            .get(track_name)
            .ok_or(anyhow::anyhow!("Name not found"))?;
        let pos = entry.track_index.query(contig, start)?;
        let f32_size = std::mem::size_of::<f32>() as u64;
        let offset = entry.gzi.query(pos * f32_size)?;
        let path = Path::new(self.dir.as_str()).join(format!("{}.track.gz", track_name));
        Ok((path, offset))
    }

    pub(crate) fn read_sequence(
        &self,
        track_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> Result<Array1<f32>> {
        let (path, pos) = self.query(track_name, contig, start)?;

        let mut reader = bgzf::Reader::new(File::open(path)?);
        reader.seek_to_virtual_position(pos)?;

        let float_size = std::mem::size_of::<f32>();
        let total_bytes = (length as usize) * float_size;
        let mut byte_buffer = vec![0; total_bytes];

        reader.read_exact(&mut byte_buffer)?;

        let buffer = byte_buffer
            .chunks_exact(float_size)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect();

        Ok(buffer)
    }
}
