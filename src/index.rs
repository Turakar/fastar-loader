mod fai;
mod gzi;

use fai::{Fai, FaiTrait};
use gzi::{Gzi, GziTrait};
use noodles::{
    bgzf::{self, io::Seek, VirtualPosition},
    fasta,
};

use anyhow::anyhow;
use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::BufRead,
    path::{Path, PathBuf},
};

use crate::util::with_suffix;

#[derive(Archive, Serialize, Deserialize)]
struct Index {
    gzi: Gzi,
    fai: Fai,
}

#[derive(Archive, Serialize, Deserialize)]
pub struct IndexMap {
    dir: String,
    map: BTreeMap<String, Index>,
}

impl IndexMap {
    pub fn build(dir: &str) -> Result<Self> {
        let mut map = BTreeMap::new();
        for fasta_result in glob::glob(format!("{}/*.fna.gz", dir).as_str())? {
            let fasta_path = fasta_result?;
            let fasta_name = fasta_path
                .file_name()
                .ok_or_else(|| anyhow!("Invalid file name"))?
                .to_str()
                .ok_or_else(|| anyhow!("Invalid UTF-8 sequence"))?
                .strip_suffix(".fna.gz")
                .ok_or_else(|| anyhow!("Invalid file name"))?
                .to_string();
            let gzi = Gzi::read(with_suffix(fasta_path.clone(), ".gzi"))?;
            let fai = Fai::read(with_suffix(fasta_path.clone(), ".fai"))?;
            let entry = Index { gzi, fai };
            map.insert(fasta_name, entry);
        }
        Ok(IndexMap {
            map,
            dir: dir.to_string(),
        })
    }
}

pub trait IndexMapTrait {
    fn names(&self) -> Vec<&str>;

    fn query(
        &self,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
    ) -> Result<(PathBuf, VirtualPosition)>;

    fn read_sequence(
        &self,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let (path, pos) = self.query(fasta_name, contig, start)?;

        // Open FASTA sequence reader at correct offset
        let mut bgzf_reader = bgzf::Reader::new(File::open(path)?);
        bgzf_reader.seek_to_virtual_position(pos)?;
        let mut fasta_reader = fasta::Reader::new(bgzf_reader);
        let mut sequence_reader = fasta_reader.sequence_reader();

        // Read until we have the desired number of nucleotides
        let mut buf = Vec::with_capacity(length as usize);
        while buf.len() < length as usize {
            let src = sequence_reader.fill_buf()?;
            if src.is_empty() {
                return Err(anyhow!(
                    "End of file / sequence reached before reading {} nucleotides",
                    length
                ));
            }
            let i = (length as usize - buf.len()).min(src.len());
            buf.extend_from_slice(&src[..i]);
            sequence_reader.consume(i);
        }
        Ok(buf)
    }
}

impl IndexMapTrait for IndexMap {
    fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    fn query(
        &self,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
    ) -> Result<(PathBuf, VirtualPosition)> {
        // Search in index
        let entry = self
            .map
            .get(fasta_name)
            .ok_or(anyhow::anyhow!("Fasta name not found"))?;
        let pos = entry.fai.query(contig, start)?;
        let offset = entry.gzi.query(pos)?;
        let path = Path::new(self.dir.as_str()).join(format!("{}.fna.gz", fasta_name));
        Ok((path, offset))
    }
}

impl IndexMapTrait for ArchivedIndexMap {
    fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    fn query(
        &self,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
    ) -> Result<(PathBuf, VirtualPosition)> {
        // Search in index
        let entry = self
            .map
            .get(fasta_name)
            .ok_or(anyhow::anyhow!("Fasta name not found"))?;
        let pos = entry.fai.query(contig, start)?;
        let offset = entry.gzi.query(pos)?;
        let path = Path::new(self.dir.as_ref()).join(format!("{}.fna.gz", fasta_name));
        Ok((path, offset))
    }
}
