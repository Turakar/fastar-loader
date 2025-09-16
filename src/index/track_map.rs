use crate::index::bgzf_index::BgzfIndex;
use crate::util::{get_relative_name_without_suffix, with_suffix};
use anyhow::Context;
use noodles::bgzf::{self, io::Seek, VirtualPosition};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use numpy::ndarray::Array1;
use rayon::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};
use std::io::Read;
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
};

use super::track_index::TrackIndex;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Index {
    gzi: BgzfIndex,
    track_index: TrackIndex,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct TrackMap {
    map: BTreeMap<String, Index>,
}

impl TrackMap {
    pub(crate) fn build(
        root: &str,
        strict: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
    ) -> Result<Self> {
        let paths = glob::glob(format!("{}/**/*.track.gz", root).as_str())?
            .map(|entry| entry.map_err(anyhow::Error::from))
            .collect::<Result<Vec<_>>>()?;
        let num_paths = paths.len();

        // Progress bar setup
        let pb = if show_progress {
            let pb = ProgressBar::new(num_paths as u64);
            pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("##-"),
            );
            Some(pb)
        } else {
            None
        };

        let build_indices = || {
            let results: Result<Vec<Option<(String, Index)>>, anyhow::Error> = paths
                .par_iter()
                .map(|track_path| {
                    let res = match Self::index_path(track_path, Path::new(root), min_contig_length)
                    {
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
                    };
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    res
                })
                .collect();
            results
        };

        let results = if let Some(workers) = num_workers {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            pool.install(build_indices)?
        } else {
            build_indices()?
        };

        if let Some(pb) = pb {
            pb.finish_with_message("Indexing complete");
        }
        let map = results.into_iter().flatten().collect::<BTreeMap<_, _>>();
        Ok(TrackMap { map })
    }

    fn index_path(
        track_path: &Path,
        root: &Path,
        min_contig_length: u64,
    ) -> Result<(String, Index)> {
        let track_name = get_relative_name_without_suffix(track_path, root, ".track.gz")?;
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
        root: &str,
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
        let offset = entry.gzi.query(pos)?;
        let path = Path::new(root).join(format!("{}.track.gz", track_name));
        Ok((path, offset))
    }

    pub(crate) fn read_sequence(
        &self,
        root: &str,
        track_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> Result<Array1<u8>> {
        let (path, pos) = self.query(root, track_name, contig, start)?;
        let mut reader = bgzf::io::Reader::new(File::open(path)?);
        reader.seek_to_virtual_position(pos)?;
        let mut byte_buffer = vec![0; length as usize];
        reader.read_exact(&mut byte_buffer)?;
        Ok(Array1::from(byte_buffer))
    }
}
