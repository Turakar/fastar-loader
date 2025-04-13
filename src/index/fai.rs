use std::path::Path;

use anyhow::Result;
use noodles::fasta::fai::Index as NoodlesIndex;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct FaiRecord {
    contig: Vec<u8>,
    length: u64,
    offset: u64,
    line_bases: u64,
    line_width: u64,
}

trait FaiRecordTrait {
    fn query(&self, start: u64) -> u64;
}

impl FaiRecordTrait for FaiRecord {
    fn query(&self, start: u64) -> u64 {
        self.offset + start / self.line_bases * self.line_width + start % self.line_bases
    }
}

impl FaiRecordTrait for ArchivedFaiRecord {
    fn query(&self, start: u64) -> u64 {
        self.offset + start / self.line_bases * self.line_width + start % self.line_bases
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct Fai {
    entries: Vec<FaiRecord>,
}

impl Fai {
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let index: NoodlesIndex = noodles::fasta::fai::read(path)?;
        Ok(Fai::from(&index))
    }
}

impl From<&NoodlesIndex> for Fai {
    fn from(index: &NoodlesIndex) -> Self {
        let entries = index
            .as_ref()
            .iter()
            .map(|record| FaiRecord {
                contig: record.name().to_vec(),
                length: record.length(),
                offset: record.offset(),
                line_bases: record.line_bases(),
                line_width: record.line_width(),
            })
            .collect();
        Fai { entries }
    }
}

pub(super) trait FaiTrait {
    fn query(&self, contig: &[u8], start: u64) -> Result<u64>;
}

impl FaiTrait for Fai {
    fn query(&self, contig: &[u8], start: u64) -> Result<u64> {
        self.entries
            .iter()
            .find(|record| record.contig.as_slice() == contig)
            .ok_or(anyhow::anyhow!("Contig not found"))
            .map(|record| record.query(start))
    }
}

impl FaiTrait for ArchivedFai {
    fn query(&self, contig: &[u8], start: u64) -> Result<u64> {
        self.entries
            .iter()
            .find(|record| record.contig.as_ref() == contig)
            .ok_or(anyhow::anyhow!("Contig not found"))
            .map(|record| record.query(start))
    }
}
