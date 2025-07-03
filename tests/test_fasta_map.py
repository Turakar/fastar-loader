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
    loader = FastarLoader(assemblies_path, no_cache=True)
    return loader


def test_names(loader: FastarLoader, expected_names: list[str]) -> None:
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_structure(loader: FastarLoader, fasta_structure: dict[str, list[tuple[str, int]]]) -> None:
    for name, contigs in fasta_structure.items():
        assert loader.contigs(name) == contigs


def test_read_sequence(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


def test_pickle(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = unpickled_loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


def test_multiprocess(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
        future = executor.submit(loader.read_sequence, name, contig, start, length)
        sequence = future.result()

    assert_array_equal(sequence, expected_sequence)


def test_cache(assemblies_path: Path) -> None:
    ref = FastarLoader(assemblies_path, no_cache=True)
    cache_path = assemblies_path / ".fasta-map-cache"

    # Load without cache
    cache_path.unlink(missing_ok=True)
    nocache = FastarLoader(assemblies_path)
    assert cache_path.exists()
    assert ref.names == nocache.names
    for name in ref.names:
        assert ref.contigs(name) == nocache.contigs(name)

    # Load with cache
    cache = FastarLoader(assemblies_path)
    assert ref.names == cache.names
    for name in ref.names:
        assert ref.contigs(name) == cache.contigs(name)

    # Clean up cache
    cache_path.unlink()


def test_min_contig_length(assemblies_path: Path, expected_names: list[str]) -> None:
    min_length = 1_000_000
    ref = FastarLoader(assemblies_path, no_cache=True)
    restricted = FastarLoader(assemblies_path, min_contig_length=min_length, no_cache=True)
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
