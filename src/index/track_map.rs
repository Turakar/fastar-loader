use crate::index::bgzf_index::BgzfIndex;
use crate::util::get_relative_name_without_suffix;
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
        names: Option<Vec<String>>,
    ) -> Result<Self> {
        let root_path = Path::new(root);
        let names = match names {
            None => glob::glob(format!("{}/**/*.track.gz", root).as_str())?
                .map(|entry| {
                    entry.map_err(anyhow::Error::from).and_then(|path| {
                        get_relative_name_without_suffix(&path, root_path, ".track.gz")
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            Some(names) => names,
        };
        let num_names = names.len();

        // Progress bar setup
        let pb = if show_progress {
            let pb = ProgressBar::new(num_names as u64);
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

        // Build indices in parallel using rayon. If num_workers is set, use a custom thread pool.
        let build_indices = || {
            let results: Result<Vec<Option<(String, Index)>>, anyhow::Error> = names
                .par_iter()
                .map(|name| {
                    let res = match Self::index_name(name, Path::new(root), min_contig_length) {
                        Ok(index) => Ok(Some((name.to_string(), index))),
                        Err(e) => {
                            if strict {
                                Err(e.context(format!("Error processing track! {}", name)))
                            } else {
                                eprintln!(
                                    "Error processing track: {}. Skipping. Error: {:?}",
                                    name, e
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

    fn index_name(name: &str, root: &Path, min_contig_length: u64) -> Result<Index> {
        let gzi = BgzfIndex::read(root.join(format!("{}.track.gz.gzi", name)))
            .context("Failed to read .gzi")?;
        let track_index = TrackIndex::read(
            root.join(format!("{}.track.gz.idx", name)),
            min_contig_length,
        )
        .context("Failed to read .idx")?;
        Ok(Index { gzi, track_index })
    }
}

impl ArchivedTrackMap {
    pub(crate) fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    pub(crate) fn contigs(&self, track_name: &str) -> Result<Vec<(&[u8], u64)>> {
        let entry = self.map.get(track_name).ok_or(anyhow::anyhow!(format!(
            "Track name not found: {}",
            track_name
        )))?;
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
