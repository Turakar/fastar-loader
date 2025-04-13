mod index;
// mod shmem;
mod util;

use anyhow::Result;
use index::{IndexMap, IndexMapTrait};
use noodles::bgzf;
use noodles::core::{Position, Region};
use noodles::fasta;
use pyo3::{exceptions::PyRuntimeError, prelude::*};

#[pyfunction]
fn read_sequence(
    fasta_path: &str,
    gzi_path: &str,
    fai_path: &str,
    chromosome: &str,
    start: usize,
    length: usize,
) -> PyResult<Vec<u8>> {
    read_sequence_(fasta_path, gzi_path, fai_path, chromosome, start, length)
        .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
}

fn read_sequence_(
    fasta_path: &str,
    gzi_path: &str,
    fai_path: &str,
    chromosome: &str,
    start: usize,
    length: usize,
) -> Result<Vec<u8>> {
    let bgzf_reader = bgzf::indexed_reader::Builder::default()
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
    Ok(sequence)
}

#[pyclass(name = "IndexMap")]
struct PyIndexMap {
    map: IndexMap,
}

#[pymethods]
impl PyIndexMap {
    #[staticmethod]
    fn build(dir: &str) -> PyResult<Self> {
        let map = IndexMap::build(dir).map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        Ok(PyIndexMap { map })
    }

    #[getter]
    fn names(&self) -> PyResult<Vec<&str>> {
        Ok(self.map.names())
    }

    fn read_sequence(
        &self,
        fasta_name: &str,
        contig: &[u8],
        start: u64,
        length: u64,
    ) -> PyResult<Vec<u8>> {
        self.map
            .read_sequence(fasta_name, contig, start, length)
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    }

    // fn to_shared_memory(&self) -> PyResult<SharedMemoryIndexMap> {
    //     let shmem = shmem::ShmemReadonlyContainer::new(&self.map)
    //         .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
    //     Ok(SharedMemoryIndexMap { shmem })
    // }
}

// #[pyclass(frozen)]
// struct SharedMemoryIndexMap {
//     shmem: shmem::ShmemReadonlyContainer<IndexMap>,
// }

// #[pymethods]
// impl SharedMemoryIndexMap {
//     #[getter]
//     fn handle(&self) -> PyResult<&str> {
//         let handle = self.shmem.get_os_id();
//         Ok(handle)
//     }

//     #[staticmethod]
//     fn from_handle(handle: &str) -> PyResult<Self> {
//         let shmem = shmem::ShmemReadonlyContainer::from_os_id(&handle)
//             .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
//         Ok(SharedMemoryIndexMap { shmem })
//     }

//     #[getter]
//     fn names(&self) -> PyResult<Vec<&str>> {
//         Ok(self.shmem.as_ref().names())
//     }

//     fn get_sequence(
//         &self,
//         fasta_name: &str,
//         contig: &[u8],
//         start: u64,
//         length: u64,
//     ) -> PyResult<Vec<u8>> {
//         self.shmem
//             .as_ref()
//             .read_sequence(fasta_name, contig, start, length)
//             .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
//     }
// }

#[pymodule]
fn fastar_loader(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(read_sequence))?;
    m.add_class::<PyIndexMap>()?;
    // m.add_class::<SharedMemoryIndexMap>()?;
    Ok(())
}
