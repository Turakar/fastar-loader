use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::Result;
use rkyv::{option::ArchivedOption, Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct TrackIndexRecord {
    name: Option<Vec<u8>>,
    offset: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct TrackIndex {
    entries: Vec<TrackIndexRecord>,
}

impl TrackIndex {
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.split(b'\n') {
            let line = line?;
            let mut fields = line.splitn(2, |&b| b == b'\t');
            let name = fields.next().and_then(|field| {
                if field.is_empty() {
                    None
                } else {
                    Some(field.to_vec())
                }
            });
            if let Some(offset_field) = fields.next() {
                if let Some(offset) = std::str::from_utf8(offset_field)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    entries.push(TrackIndexRecord { name, offset });
                }
            }
        }

        Ok(TrackIndex { entries })
    }
}

pub(super) trait TrackIndexTrait {
    fn query(&self, name: &[u8], start: u64) -> Result<u64>;
}

impl TrackIndexTrait for TrackIndex {
    fn query(&self, name: &[u8], start: u64) -> Result<u64> {
        let i = self.entries.iter().find(|r| match &r.name {
            Some(entry_name) => entry_name == name,
            None => false,
        });
        match i {
            Some(entry) => Ok(entry.offset + start),
            None => Err(anyhow::anyhow!(
                "Track not found: {}",
                String::from_utf8_lossy(name)
            )),
        }
    }
}

impl TrackIndexTrait for ArchivedTrackIndex {
    fn query(&self, name: &[u8], start: u64) -> Result<u64> {
        let i = self.entries.iter().find(|r| match &r.name {
            ArchivedOption::Some(entry_name) => entry_name == name,
            ArchivedOption::None => false,
        });
        match i {
            Some(entry) => Ok(u64::from(entry.offset) + start),
            None => Err(anyhow::anyhow!(
                "Track not found: {}",
                String::from_utf8_lossy(name)
            )),
        }
    }
}
