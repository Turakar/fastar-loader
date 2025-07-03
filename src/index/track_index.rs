use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct TrackIndexRecord {
    name: Vec<u8>,
    offset: u64,
    length: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(super) struct TrackIndex {
    entries: Vec<TrackIndexRecord>,
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

        Ok(TrackIndex { entries })
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
        let i = self.entries.iter().find(|r| r.name.as_slice() == name);
        match i {
            Some(entry) => Ok(u64::from(entry.offset) + start),
            None => Err(anyhow::anyhow!(
                "Track not found: {}",
                String::from_utf8_lossy(name)
            )),
        }
    }
}
