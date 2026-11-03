use std::path::Path;

use anyhow::Result;
use noodles::fasta::fai::Index as NoodlesIndex;
use rkyv::{Archive, Deserialize, Serialize};

use super::by_name::{find_by_name, positions_by_name};

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
    /// Positions in `entries` ordered by contig name, for the binary search in `query`.
    by_name: Vec<u32>,
}

impl FastaIndex {
    pub(super) fn read<P: AsRef<Path>>(path: P, min_contig_length: u64) -> Result<Self> {
        let index: NoodlesIndex = noodles::fasta::fai::fs::read(path)?;
        FastaIndex::from_noodles(&index, min_contig_length)
    }
}

impl FastaIndex {
    fn from_noodles(index: &NoodlesIndex, min_contig_length: u64) -> Result<Self> {
        let entries: Vec<Record> = index
            .as_ref()
            .iter()
            .map(|record| Record {
                contig: record.name().to_vec(),
                length: record.length(),
                offset: record.position(),
                line_bases: record.line_base_count().get(),
                line_width: record.line_width().get(),
            })
            .filter(|record| record.length >= min_contig_length)
            .collect();
        let by_name = positions_by_name(&entries, |record| &record.contig)?;
        Ok(FastaIndex { entries, by_name })
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
        find_by_name(&self.entries, &self.by_name, contig, |record| {
            record.contig.as_ref()
        })
        .ok_or(anyhow::anyhow!("Contig not found"))
        .map(|record| {
            record.offset
                + start / record.line_bases * record.line_width
                + start % record.line_bases
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::rancor;

    #[test]
    fn test_query_every_contig() {
        let index = FastaIndex::read("test-data/assemblies/GCA_000146045.2.fna.gz.fai", 0).unwrap();
        let bytes = rkyv::to_bytes::<rancor::Error>(&index).unwrap();
        let archived = rkyv::access::<ArchivedFastaIndex, rancor::Error>(&bytes).unwrap();
        assert!(!index.entries.is_empty());
        for record in &index.entries {
            assert_eq!(archived.query(&record.contig, 0).unwrap(), record.offset);
        }
        assert!(archived.query(b"no such contig", 0).is_err());
    }
}
