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
pub(crate) struct FastaMap {
    map: BTreeMap<String, Index>,
}

impl FastaMap {
    pub(crate) fn build(root: &str, strict: bool, min_contig_length: u64) -> Result<Self> {
        let mut map = BTreeMap::new();
        let paths = glob::glob(format!("{}/*.fna.gz", root).as_str())?
            .map(|entry| entry.map_err(anyhow::Error::from))
            .collect::<Result<Vec<_>>>()?;
        let num_paths = paths.len();
        for (i, map_path) in paths.into_iter().enumerate() {
            if i % 100 == 0 && num_paths > 100 {
                eprintln!("Processed {}/{} FASTA indices", i, num_paths,);
            }
            match Self::index_path(&map_path, min_contig_length) {
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
        eprintln!("Processed {} FASTA indices", num_paths);
        Ok(FastaMap { map })
    }

    fn index_path(fasta_path: &Path, min_contig_length: u64) -> Result<(String, Index)> {
        let fasta_name = get_name_without_suffix(fasta_path, ".fna.gz")?;
        let gzi = BgzfIndex::read(with_suffix(fasta_path.to_path_buf(), ".gzi"))
            .context("Failed to read .gzi")?;
        let fai = FastaIndex::read(
            with_suffix(fasta_path.to_path_buf(), ".fai"),
            min_contig_length,
        )
        .context("Failed to read .fai")?;
        Ok((fasta_name, Index { gzi, fai }))
    }
}

impl ArchivedFastaMap {
    pub(crate) fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    pub(crate) fn contigs(&self, name: &str) -> Result<Vec<(&[u8], u64)>> {
        let entry = self
            .map
            .get(name)
            .ok_or(anyhow::anyhow!("Fasta name not found"))?;
        Ok(entry.fai.contigs())
    }

    pub(crate) fn query(
        &self,
        root: &str,
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
        let path = Path::new(root).join(format!("{}.fna.gz", fasta_name));
        Ok((path, offset))
    }

    pub(crate) fn read_sequence(
        &self,
        root: &str,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> Result<Array1<u8>> {
        let (path, pos) = self.query(root, fasta_name, contig, start)?;

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
