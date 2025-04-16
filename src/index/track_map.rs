use crate::index::bgzf_index::{BgzfIndex, BgzfIndexTrait};
use noodles::bgzf::{self, io::Seek, VirtualPosition};

use anyhow::anyhow;
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
use super::track_index::TrackIndexTrait;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Index {
    gzi: BgzfIndex,
    track_index: TrackIndex,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct TrackMap {
    dir: String,
    map: BTreeMap<String, Index>,
}

impl TrackMap {
    pub fn build(dir: &str) -> Result<Self> {
        let mut map = BTreeMap::new();
        for track_result in glob::glob(format!("{}/*.track.gz", dir).as_str())? {
            let track_path = track_result?;
            let track_name = track_path
                .file_name()
                .ok_or_else(|| anyhow!("Invalid file name"))?
                .to_str()
                .ok_or_else(|| anyhow!("Invalid UTF-8 sequence"))?
                .strip_suffix(".track.gz")
                .ok_or_else(|| anyhow!("Invalid file name"))?
                .to_string();
            let gzi = BgzfIndex::read(with_suffix(track_path.clone(), ".gzi"))?;
            let track_index = TrackIndex::read(with_suffix(track_path.clone(), ".idx"))?;
            let entry = Index { gzi, track_index };
            map.insert(track_name, entry);
        }
        Ok(TrackMap {
            map,
            dir: dir.to_string(),
        })
    }
}

pub trait TrackMapTrait {
    fn names(&self) -> Vec<&str>;

    fn contigs(&self, track_name: &str) -> Result<Vec<(&[u8], u64)>>;

    fn query(
        &self,
        track_name: &str,
        contig: &[u8],
        start: u64,
    ) -> Result<(PathBuf, VirtualPosition)>;

    fn read_sequence(
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

impl TrackMapTrait for TrackMap {
    fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    fn contigs(&self, track_name: &str) -> Result<Vec<(&[u8], u64)>> {
        let entry = self
            .map
            .get(track_name)
            .ok_or(anyhow::anyhow!("Track name not found"))?;
        Ok(entry.track_index.contigs())
    }

    fn query(
        &self,
        track_name: &str,
        contig: &[u8],
        start: u64,
    ) -> Result<(PathBuf, VirtualPosition)> {
        // Search in index
        let entry = self
            .map
            .get(track_name)
            .ok_or(anyhow::anyhow!("Fasta name not found"))?;
        let pos = entry.track_index.query(contig, start)?;
        let f32_size = std::mem::size_of::<f32>() as u64;
        let offset = entry.gzi.query(pos * f32_size)?;
        let path = Path::new(self.dir.as_str()).join(format!("{}.track.gz", track_name));
        Ok((path, offset))
    }
}

impl TrackMapTrait for ArchivedTrackMap {
    fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    fn contigs(&self, track_name: &str) -> Result<Vec<(&[u8], u64)>> {
        let entry = self
            .map
            .get(track_name)
            .ok_or(anyhow::anyhow!("Track name not found"))?;
        Ok(entry.track_index.contigs())
    }

    fn query(
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_archive() {
        let data = TrackMap::build("test-data/tracks").unwrap();
        let bytes: rkyv::util::AlignedVec = rkyv::to_bytes::<rkyv::rancor::Error>(&data).unwrap();
        println!("Data pointer: {:#x}", &bytes.as_ptr().addr());
        let archive =
            rkyv::access::<ArchivedTrackMap, rkyv::rancor::Error>(bytes.as_ref()).unwrap();
        assert_eq!(archive.names(), data.names());
    }
}
