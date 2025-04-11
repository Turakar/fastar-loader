use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufRead;
use std::path::Path;

use noodles::bgzf;
use noodles::fasta;

use anyhow::{anyhow, Result};

use crate::util::with_suffix;

struct IndexMapEntry {
    pub gzi: bgzf::gzi::Index,
    pub fai: fasta::fai::Index,
}

pub struct IndexMap {
    dir: String,
    map: BTreeMap<String, IndexMapEntry>,
}

impl IndexMap {
    pub fn build(dir: &str) -> Result<Self> {
        let mut map = BTreeMap::new();
        for fasta_result in glob::glob(format!("{}/*.fna.gz", dir).as_str())? {
            let fasta_path = fasta_result?;
            let gzi = bgzf::gzi::read(with_suffix(fasta_path.clone(), ".gzi"))?;
            let fai = fasta::fai::read(with_suffix(fasta_path.clone(), ".fai"))?;
            let fasta_name = fasta_path
                .file_name()
                .ok_or_else(|| anyhow!("Invalid file name"))?
                .to_str()
                .ok_or_else(|| anyhow!("Invalid UTF-8 sequence"))?
                .strip_suffix(".fna.gz")
                .ok_or_else(|| anyhow!("Invalid file name"))?
                .to_string();
            let entry = IndexMapEntry { gzi, fai };
            map.insert(fasta_name, entry);
        }
        Ok(IndexMap {
            map,
            dir: dir.to_string(),
        })
    }

    pub fn names(&self) -> Vec<&str> {
        self.map.keys().map(|s| s.as_str()).collect()
    }

    pub fn get_sequence(
        &self,
        fasta_name: &str,
        contig: &str,
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        // Find the indices
        let entry = self
            .map
            .get(fasta_name)
            .ok_or_else(|| anyhow!("Fasta file {} not found", fasta_name))?;
        let gzi = &entry.gzi;
        let fai = &entry.fai;

        // Find uncompressed offset (based on noodles::fasta::Reader code)
        let contig_bytes = contig.as_bytes();
        let index_record = fai
            .iter()
            .find(|record| record.name() == contig_bytes)
            .ok_or_else(|| anyhow!("Contig {} not found!", contig))?;
        let pos = index_record.offset()
            + start / index_record.line_bases() * index_record.line_width()
            + start % index_record.line_bases();

        // Open FASTA sequence reader at correct offset
        let path = Path::new(&self.dir).join(format!("{}.fna.gz", fasta_name));
        let mut bgzf_reader = bgzf::Reader::new(File::open(path)?);
        bgzf_reader.seek_by_uncompressed_position(gzi, pos)?;
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
