use std::path::Path;

use anyhow::Result;
use noodles::fasta::fai::Index as NoodlesIndex;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Record {
    contig: Vec<u8>,
    length: u64,
    offset: u64,
    line_bases: u64,
    line_width: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct FastaIndex {
    entries: Vec<Record>,
}

impl FastaIndex {
    pub(super) fn read<P: AsRef<Path>>(path: P, min_contig_length: u64) -> Result<Self> {
        let index: NoodlesIndex = noodles::fasta::fai::fs::read(path)?;
        Ok(FastaIndex::from_noodles(&index, min_contig_length))
    }
}

impl FastaIndex {
    fn from_noodles(index: &NoodlesIndex, min_contig_length: u64) -> Self {
        let entries = index
            .as_ref()
            .iter()
            .map(|record| Record {
                contig: record.name().to_vec(),
                length: record.length(),
                offset: record.offset(),
                line_bases: record.line_bases(),
                line_width: record.line_width(),
            })
            .filter(|record| record.length >= min_contig_length)
            .collect();
        FastaIndex { entries }
    }
}

impl ArchivedFastaIndex {
    pub(super) fn contigs(&self) -> Vec<(&[u8], u64)> {
        self.entries
            .iter()
            .map(|record| (record.contig.as_ref(), u64::from(record.length)))
            .collect()
    }

    pub(super) fn query(&self, contig: &[u8], start: u64) -> Result<u64> {
        self.entries
            .iter()
            .find(|record| record.contig.as_ref() == contig)
            .ok_or(anyhow::anyhow!("Contig not found"))
            .map(|record| {
                record.offset
                    + start / record.line_bases * record.line_width
                    + start % record.line_bases
            })
    }
}
