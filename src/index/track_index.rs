use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize};

use super::by_name::{find_by_name, positions_by_name};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct TrackIndexRecord {
    name: Vec<u8>,
    offset: u64,
    length: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct TrackIndex {
    entries: Vec<TrackIndexRecord>,
    /// Positions in `entries` ordered by contig name, for the binary search in `query`.
    by_name: Vec<u32>,
}

impl TrackIndex {
    pub(super) fn read<P: AsRef<Path>>(path: P, min_contig_length: u64) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        // Read names and offsets from the file
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
                    entries.push((name, offset));
                }
            }
        }

        // Create entries by computing length of neighboring offsets
        let entries = entries
            .windows(2)
            .map(|pair| {
                if let [(Some(name), offset), (_, next_offset)] = pair {
                    Ok(TrackIndexRecord {
                        name: name.clone(),
                        offset: *offset,
                        length: next_offset - offset,
                    })
                } else {
                    Err(anyhow::anyhow!("Invalid track index format"))
                }
            })
            .filter(|r| match r {
                Ok(record) => record.length >= min_contig_length,
                Err(_) => true,
            })
            .collect::<Result<Vec<_>>>()?;
        let by_name = positions_by_name(&entries, |record| &record.name)?;

        Ok(TrackIndex { entries, by_name })
    }
}

impl ArchivedTrackIndex {
    pub(super) fn contigs(&self) -> Vec<(&[u8], u64)> {
        self.entries
            .iter()
            .map(|entry| (&entry.name[..], u64::from(entry.length)))
            .collect()
    }

    pub(super) fn query(&self, name: &[u8], start: u64) -> Result<u64> {
        let i = find_by_name(&self.entries, &self.by_name, name, |r| r.name.as_slice());
        match i {
            Some(entry) => Ok(u64::from(entry.offset) + start),
            None => Err(anyhow::anyhow!(
                "Track not found: {}",
                String::from_utf8_lossy(name)
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::rancor;

    #[test]
    fn test_query_every_contig() {
        let index = TrackIndex::read("test-data/tracks/GCA_000146045.2.track.gz.idx", 0).unwrap();
        let bytes = rkyv::to_bytes::<rancor::Error>(&index).unwrap();
        let archived = rkyv::access::<ArchivedTrackIndex, rancor::Error>(&bytes).unwrap();
        assert!(!index.entries.is_empty());
        for record in &index.entries {
            assert_eq!(archived.query(&record.name, 5).unwrap(), record.offset + 5);
        }
        assert!(archived.query(b"no such contig", 0).is_err());
    }
}
