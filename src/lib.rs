use anyhow::Result;
use noodles::bgzf;
use noodles::core::{Position, Region};
use noodles::fasta;
use pyo3::{exceptions::PyRuntimeError, prelude::*};

#[pyfunction]
fn fasta_read(
    fasta_path: &str,
    gzi_path: &str,
    fai_path: &str,
    chromosome: &str,
    start: usize,
    length: usize,
) -> PyResult<Vec<u8>> {
    fasta_read_(fasta_path, gzi_path, fai_path, chromosome, start, length)
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
}

fn fasta_read_(
    fasta_path: &str,
    gzi_path: &str,
    fai_path: &str,
    chromosome: &str,
    start: usize,
    end: usize,
) -> Result<Vec<u8>> {
    let bgzf_reader = bgzf::indexed_reader::Builder::default()
        .set_index(bgzf::gzi::read(gzi_path)?)
        .build_from_path(fasta_path)?;
    let mut fasta_reader = fasta::indexed_reader::Builder::default()
        .set_index(fasta::fai::read(fai_path)?)
        .build_from_reader(bgzf_reader)?;
    let region = make_interval(chromosome, start, end)?;
    let record = fasta_reader.query(&region)?;
    let sequence = record.sequence().as_ref().to_vec();
    Ok(sequence)
}

fn make_interval(chromosome: &str, start: usize, end: usize) -> Result<Region> {
    let start_pos = Position::try_from(start + 1)?;
    let end_pos = Position::try_from(end)?;
    Ok(Region::new(chromosome, start_pos..=end_pos))
}

#[pymodule]
fn fastar_loader(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(fasta_read))?;

    Ok(())
}
