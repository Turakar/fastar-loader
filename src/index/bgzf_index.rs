use std::path::Path;

use anyhow::Result;
use noodles::bgzf::gzi::Index as NoodlesIndex;
use noodles::bgzf::VirtualPosition;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Record {
    compressed: u64,
    uncompressed: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct BgzfIndex {
    entries: Vec<Record>,
}

impl BgzfIndex {
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let index: NoodlesIndex = noodles::bgzf::gzi::read(path)?;
        Ok(BgzfIndex::from(&index))
    }
}

impl From<&NoodlesIndex> for BgzfIndex {
    fn from(index: &NoodlesIndex) -> Self {
        let entries = index
            .as_ref()
            .iter()
            .map(|(compressed, uncompressed)| Record {
                compressed: *compressed,
                uncompressed: *uncompressed,
            })
            .collect();
        BgzfIndex { entries }
    }
}

impl ArchivedBgzfIndex {
    pub fn query(&self, pos: u64) -> Result<VirtualPosition> {
        let i = self.entries.partition_point(|r| r.uncompressed <= pos);
        let (compressed, uncompressed) = match i {
            0 => (0u64, 0u64),
            i => {
                let entry = &self.entries[i - 1];
                (u64::from(entry.compressed), u64::from(entry.uncompressed))
            }
        };
        let block_data_pos = u16::try_from(pos - uncompressed)?;
        Ok(VirtualPosition::try_from((compressed, block_data_pos))?)
    }
}
