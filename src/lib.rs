mod cache;
mod index;
mod shmem;
mod util;

use anyhow::Result;
use index::{FastaMap, TrackMap};
use noodles::bgzf;
use noodles::core::{Position, Region};
use noodles::fasta;
use numpy::ndarray::Array1;
use numpy::{IntoPyArray, PyArray1};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use shmem::ShmemArchive;

#[pyfunction]
fn read_sequence<'py>(
    py: Python<'py>,
    fasta_path: &str,
    gzi_path: &str,
    fai_path: &str,
    chromosome: &str,
    start: usize,
    length: usize,
) -> PyResult<Bound<'py, PyArray1<u8>>> {
    read_sequence_(fasta_path, gzi_path, fai_path, chromosome, start, length)
        .map(|arr| arr.into_pyarray(py))
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
}

fn read_sequence_(
    fasta_path: &str,
    gzi_path: &str,
    fai_path: &str,
    chromosome: &str,
    start: usize,
    length: usize,
) -> Result<Array1<u8>> {
    let bgzf_reader = bgzf::io::indexed_reader::Builder::default()
        .set_index(bgzf::gzi::read(gzi_path)?)
        .build_from_path(fasta_path)?;
    let mut fasta_reader = fasta::indexed_reader::Builder::default()
        .set_index(fasta::fai::read(fai_path)?)
        .build_from_reader(bgzf_reader)?;
    let start_pos = Position::try_from(start + 1)?;
    let end_pos = Position::try_from(start + length)?;
    let region = Region::new(chromosome, start_pos..=end_pos);
    let record = fasta_reader.query(&region)?;
    let sequence = record.sequence().as_ref().to_vec();
    Ok(sequence.into())
}

#[pyclass(frozen, name = "FastaMap")]
struct PyFastaMap {
    shmem: ShmemArchive<FastaMap>,
    root: String,
}

#[pymethods]
impl PyFastaMap {
    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    fn load(
        py: Python,
        root: &str,
        strict: bool,
        force_build: bool,
        no_cache: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            cache::load_fasta_map(
                root,
                strict,
                force_build,
                no_cache,
                min_contig_length,
                num_workers,
                show_progress,
            )
        })
        .map(|shmem| PyFastaMap {
            shmem,
            root: root.to_string(),
        })
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }

    #[getter]
    fn handle(&self) -> PyResult<&str> {
        let handle = self.shmem.get_os_id();
        Ok(handle)
    }

    #[getter]
    fn root(&self) -> PyResult<&str> {
        Ok(&self.root)
    }

    #[staticmethod]
    fn from_handle(handle: &str, root: &str) -> PyResult<Self> {
        ShmemArchive::from_os_id(handle)
            .map(|shmem| PyFastaMap {
                shmem,
                root: root.to_string(),
            })
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }

    #[getter]
    fn names(&self) -> PyResult<Vec<&str>> {
        Ok(self.shmem.as_ref().names())
    }

    fn contigs(&self, fasta_name: &str) -> PyResult<Vec<(&[u8], u64)>> {
        self.shmem
            .as_ref()
            .contigs(fasta_name)
            .map_err(|e| PyRuntimeError::new_err(format!("Error getting contigs: {:?}", e)))
    }

    fn read_sequence<'py>(
        &self,
        py: Python<'py>,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> PyResult<Bound<'py, PyArray1<u8>>> {
        py.allow_threads(|| {
            self.shmem
                .as_ref()
                .read_sequence(&self.root, fasta_name, contig, start, length)
        })
        .map(|arr| arr.into_pyarray(py))
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }
}

#[pyclass(frozen, name = "TrackMap")]
struct PyTrackMap {
    shmem: ShmemArchive<TrackMap>,
    root: String,
}

#[pymethods]
impl PyTrackMap {
    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    fn load(
        py: Python,
        root: &str,
        strict: bool,
        force_build: bool,
        no_cache: bool,
        min_contig_length: u64,
        num_workers: Option<usize>,
        show_progress: bool,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            cache::load_track_map(
                root,
                strict,
                force_build,
                no_cache,
                min_contig_length,
                num_workers,
                show_progress,
            )
        })
        .map(|shmem| PyTrackMap {
            shmem,
            root: root.to_string(),
        })
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }

    #[getter]
    fn handle(&self) -> PyResult<&str> {
        Ok(self.shmem.get_os_id())
    }

    #[getter]
    fn root(&self) -> PyResult<&str> {
        Ok(&self.root)
    }

    #[staticmethod]
    fn from_handle(handle: &str, root: &str) -> PyResult<Self> {
        ShmemArchive::from_os_id(handle)
            .map(|shmem| PyTrackMap {
                shmem,
                root: root.to_string(),
            })
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }

    #[getter]
    fn names(&self) -> PyResult<Vec<&str>> {
        Ok(self.shmem.as_ref().names())
    }

    fn contigs(&self, fasta_name: &str) -> PyResult<Vec<(&[u8], u64)>> {
        self.shmem
            .as_ref()
            .contigs(fasta_name)
            .map_err(|e| PyRuntimeError::new_err(format!("Error getting contigs: {:?}", e)))
    }

    fn read_sequence<'py>(
        &self,
        py: Python<'py>,
        track_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> PyResult<Bound<'py, PyArray1<u8>>> {
        py.allow_threads(|| {
            self.shmem
                .as_ref()
                .read_sequence(&self.root, track_name, contig, start, length)
        })
        .map(|arr| arr.into_pyarray(py))
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }
}

#[pymodule]
fn fastar_loader(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(read_sequence))?;
    m.add_class::<PyFastaMap>()?;
    m.add_class::<PyTrackMap>()?;
    Ok(())
}
