use crate::index::bgzf_index::BgzfIndex;
use crate::util::get_name_without_suffix;
use anyhow::Context;
use noodles::bgzf::{self, io::Seek, VirtualPosition};

use anyhow::Result;
use numpy::ndarray::Array1;
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
    map: BTreeMap<String, Index>,
}

impl TrackMap {
    pub(crate) fn build(root: &str, strict: bool, min_contig_length: u64) -> Result<Self> {
        let mut map = BTreeMap::new();
        let paths = glob::glob(format!("{}/*.track.gz", root).as_str())?
            .map(|entry| entry.map_err(anyhow::Error::from))
            .collect::<Result<Vec<_>>>()?;
        let num_paths = paths.len();
        for (i, track_path) in paths.into_iter().enumerate() {
            if i % 100 == 0 && num_paths > 100 {
                eprintln!("Processed {}/{} track indices", i, num_paths,);
            }
            match Self::index_path(&track_path, min_contig_length) {
                Ok((track_name, index)) => {
                    map.insert(track_name, index);
                }
                Err(e) => {
                    if strict {
                        return Err(e);
                    } else {
                        eprintln!(
                            "Error processing track: {}. Skipping. Error: {:?}",
                            track_path.display(),
                            e
                        );
                    }
                }
            }
        }
        eprintln!("Processed {} track indices", num_paths);
        Ok(TrackMap { map })
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
        let mut reader = bgzf::Reader::new(File::open(path)?);
        reader.seek_to_virtual_position(pos)?;
        let mut byte_buffer = vec![0; length as usize];
        reader.read_exact(&mut byte_buffer)?;
        Ok(Array1::from(byte_buffer))
    }
}
