import multiprocessing
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pytest
from fastar_loader import FastarLoader
from numpy.testing import assert_array_equal


@pytest.fixture()
def loader(assemblies_path: Path) -> FastarLoader:
    loader = FastarLoader(assemblies_path, no_cache=True, storage_method="memory")
    return loader


def test_names(loader: FastarLoader, expected_names: list[str]) -> None:
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_structure(loader: FastarLoader, fasta_structure: dict[str, list[tuple[str, int]]]) -> None:
    for name, contigs in fasta_structure.items():
        assert loader.contigs(name) == contigs


def test_custom_num_workers(
    assemblies_path: Path,
    expected_names: list[str],
    fasta_structure: dict[str, list[tuple[str, int]]],
) -> None:
    loader = FastarLoader(assemblies_path, no_cache=True, storage_method="memory", num_workers=2)
    names = loader.names
    assert len(names) == len(expected_names)
    for name, contigs in fasta_structure.items():
        assert loader.contigs(name) == contigs


def test_read_sequence(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


@pytest.mark.parametrize("storage_method", ["shmem", "mmap"])
def test_pickle(
    assemblies_path: Path,
    fasta_test_data: tuple[Path, str, str, int, int, np.ndarray],
    storage_method: str,
) -> None:
    clean_cache(assemblies_path)
    loader = FastarLoader(assemblies_path, no_cache=False, storage_method=storage_method)
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = unpickled_loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)
    clean_cache(assemblies_path)


@pytest.mark.parametrize("storage_method", ["shmem", "mmap"])
def test_multiprocess(
    assemblies_path: Path,
    fasta_test_data: tuple[Path, str, str, int, int, np.ndarray],
    storage_method: str,
) -> None:
    clean_cache(assemblies_path)
    loader = FastarLoader(assemblies_path, no_cache=False, storage_method=storage_method)
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
        future = executor.submit(loader.read_sequence, name, contig, start, length)
        sequence = future.result()

    assert_array_equal(sequence, expected_sequence)
    clean_cache(assemblies_path)


@pytest.mark.parametrize("storage_method", ["shmem", "mmap", "memory"])
def test_cache(assemblies_path: Path, storage_method: str) -> None:
    ref = FastarLoader(assemblies_path, no_cache=True, storage_method="memory")

    # Load without cache
    clean_cache(assemblies_path)
    nocache = FastarLoader(assemblies_path, storage_method=storage_method)
    assert len(list(assemblies_path.glob(".fasta-map-cache-*"))) == 1
    assert ref.names == nocache.names
    for name in ref.names:
        assert ref.contigs(name) == nocache.contigs(name)

    # Load with cache
    cache = FastarLoader(assemblies_path, storage_method=storage_method)
    assert ref.names == cache.names
    for name in ref.names:
        assert ref.contigs(name) == cache.contigs(name)

    # Clean up cache
    clean_cache(assemblies_path)


def clean_cache(assemblies_path: Path) -> None:
    for cache_file in assemblies_path.glob(".fasta-map-cache-*"):
        cache_file.unlink(missing_ok=True)


def test_min_contig_length(assemblies_path: Path, expected_names: list[str]) -> None:
    min_length = 1_000_000
    ref = FastarLoader(assemblies_path, no_cache=True, storage_method="memory")
    restricted = FastarLoader(
        assemblies_path, min_contig_length=min_length, no_cache=True, storage_method="memory"
    )
    for name in expected_names:
        ref_contigs = ref.contigs(name)
        restricted_contigs = restricted.contigs(name)
        for contig, length in ref_contigs:
            if length >= min_length:
                assert (contig, length) in restricted_contigs
            else:
                assert (contig, length) not in restricted_contigs
        for contig, length in restricted_contigs:
            assert (contig, length) in ref_contigs
