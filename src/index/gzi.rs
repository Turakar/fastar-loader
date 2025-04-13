use std::path::Path;

use anyhow::Result;
use noodles::bgzf::gzi::Index as NoodlesIndex;
use noodles::bgzf::VirtualPosition;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct GziRecord {
    compressed: u64,
    uncompressed: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct Gzi {
    entries: Vec<GziRecord>,
}

impl Gzi {
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let index: NoodlesIndex = noodles::bgzf::gzi::read(path)?;
        Ok(Gzi::from(&index))
    }
}

impl From<&NoodlesIndex> for Gzi {
    fn from(index: &NoodlesIndex) -> Self {
        let entries = index
            .iter()
            .map(|(compressed, uncompressed)| GziRecord {
                compressed: *compressed,
                uncompressed: *uncompressed,
            })
            .collect();
        Gzi { entries }
    }
}

pub(super) trait GziTrait {
    fn query(&self, pos: u64) -> Result<VirtualPosition>;
}

impl GziTrait for Gzi {
    fn query(&self, pos: u64) -> Result<VirtualPosition> {
        let i = self.entries.partition_point(|r| r.uncompressed <= pos);
        let (compressed, uncompressed) = match i {
            0 => (0u64, 0u64),
            i => {
                let entry = &self.entries[i - 1];
                (entry.compressed, entry.uncompressed)
            }
        };
        let block_data_pos = u16::try_from(pos - uncompressed)?;
        Ok(VirtualPosition::try_from((compressed, block_data_pos))?)
    }
}

impl GziTrait for ArchivedGzi {
    fn query(&self, pos: u64) -> Result<VirtualPosition> {
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
