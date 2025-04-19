use crate::index::bgzf_index::BgzfIndex;
use crate::index::fasta_index::FastaIndex;
use noodles::{
    bgzf::{self, io::Seek, VirtualPosition},
    fasta,
};

use anyhow::Result;
use anyhow::{anyhow, Context};
use numpy::ndarray::Array1;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::BufRead,
    path::{Path, PathBuf},
};

use crate::util::{get_name_without_suffix, with_suffix};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Index {
    gzi: BgzfIndex,
    fai: FastaIndex,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct FastaMap {
    dir: String,
    map: BTreeMap<String, Index>,
}

impl FastaMap {
    pub fn build(dir: &str, strict: bool) -> Result<Self> {
        let mut map = BTreeMap::new();
        for map_result in glob::glob(format!("{}/*.fna.gz", dir).as_str())? {
            let map_path = map_result?;
            match Self::index_path(&map_path) {
                Ok((track_name, index)) => {
                    map.insert(track_name, index);
                }
                Err(e) => {
                    if strict {
                        return Err(e)
                            .context(format!("Error processing track! {}", map_path.display()));
                    } else {
                        eprintln!(
                            "Error processing track: {}. Skipping. Error: {:?}",
                            map_path.display(),
                            e
                        );
                    }
                }
            }
        }
        Ok(FastaMap {
            map,
            dir: dir.to_string(),
        })
    }

    fn index_path(fasta_path: &Path) -> Result<(String, Index)> {
        let fasta_name = get_name_without_suffix(fasta_path, ".fna.gz")?;
        let gzi = BgzfIndex::read(with_suffix(fasta_path.to_path_buf(), ".gzi"))
            .context("Failed to read .gzi")?;
        let fai = FastaIndex::read(with_suffix(fasta_path.to_path_buf(), ".fai"))
            .context("Failed to read .fai")?;
        Ok((fasta_name, Index { gzi, fai }))
    }
}

impl ArchivedFastaMap {
    pub fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    pub fn contigs(&self, name: &str) -> Result<Vec<(&[u8], u64)>> {
        let entry = self
            .map
            .get(name)
            .ok_or(anyhow::anyhow!("Fasta name not found"))?;
        Ok(entry.fai.contigs())
    }

    pub fn query(
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

    pub fn read_sequence(
        &self,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> Result<Array1<u8>> {
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
        Ok(buf.into())
    }
}
