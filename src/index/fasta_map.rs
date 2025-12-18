use crate::index::bgzf_index::BgzfIndex;
use crate::index::fasta_index::FastaIndex;
use noodles::{
    bgzf::{self, io::Seek, VirtualPosition},
    fasta,
};

use anyhow::Result;
use anyhow::{anyhow, Context};
use indicatif::{ProgressBar, ProgressStyle};
use numpy::ndarray::Array1;
use rayon::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::BufRead,
    path::{Path, PathBuf},
};

use crate::util::get_relative_name_without_suffix;

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
    pub(crate) fn build(
        root: &str,
        strict: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
        names: Option<Vec<String>>,
    ) -> Result<Self> {
        let root_path = Path::new(root);
        let names = match names {
            None => glob::glob(format!("{}/**/*.fna.gz", root).as_str())?
                .map(|entry| {
                    entry.map_err(anyhow::Error::from).and_then(|path| {
                        get_relative_name_without_suffix(&path, root_path, ".fna.gz")
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            Some(names) => names,
        };
        let num_names = names.len();

        // Progress bar setup
        let pb = if show_progress {
            let pb = ProgressBar::new(num_names as u64);
            pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("##-"),
            );
            Some(pb)
        } else {
            None
        };

        // Build indices in parallel using rayon. If num_workers is set, use a custom thread pool.
        let build_indices = || {
            let results: Result<Vec<Option<(String, Index)>>, anyhow::Error> = names
                .par_iter()
                .map(|name| {
                    let res = match Self::index_name(name, Path::new(root), min_contig_length) {
                        Ok(index) => Ok(Some((name.to_string(), index))),
                        Err(e) => {
                            if strict {
                                Err(e.context(format!("Error processing track! {}", name)))
                            } else {
                                eprintln!(
                                    "Error processing track: {}. Skipping. Error: {:?}",
                                    name, e
                                );
                                Ok(None)
                            }
                        }
                    };
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    res
                })
                .collect();
            results
        };

        let results = if let Some(workers) = num_workers {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            pool.install(build_indices)?
        } else {
            build_indices()?
        };

        if let Some(pb) = pb {
            pb.finish_with_message("Indexing complete");
        }
        let map = results.into_iter().flatten().collect::<BTreeMap<_, _>>();
        Ok(FastaMap { map })
    }

    fn index_name(name: &str, root: &Path, min_contig_length: u64) -> Result<Index> {
        let gzi = BgzfIndex::read(root.join(format!("{}.fna.gz.gzi", name)))
            .context("Failed to read .gzi")?;
        let fai = FastaIndex::read(root.join(format!("{}.fna.gz.fai", name)), min_contig_length)
            .context("Failed to read .fai")?;
        Ok(Index { gzi, fai })
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
            .ok_or(anyhow::anyhow!(format!("Fasta name not found: {}", name)))?;
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
        let mut bgzf_reader = bgzf::io::Reader::new(File::open(path)?);
        bgzf_reader.seek_to_virtual_position(pos)?;
        let mut fasta_reader = fasta::io::Reader::new(bgzf_reader);
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
