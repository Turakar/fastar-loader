import multiprocessing
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pytest
from fastar_loader import TrackLoader
from numpy.testing import assert_array_equal


@pytest.fixture()
def loader(tracks_path: Path) -> TrackLoader:
    loader = TrackLoader(tracks_path)
    loader.index()
    return loader


def test_names(loader: TrackLoader, expected_names: list[str]) -> None:
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_names_shmem(loader: TrackLoader, expected_names: list[str]) -> None:
    loader.to_shared_memory()
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_structure(loader: TrackLoader, track_structure: dict[str, list[tuple[str, int]]]) -> None:
    for name, contigs in track_structure.items():
        assert loader.contigs(name) == contigs


def test_structure_shmem(
    loader: TrackLoader, track_structure: dict[str, list[tuple[str, int]]]
) -> None:
    loader.to_shared_memory()
    for name, contigs in track_structure.items():
        assert loader.contigs(name) == contigs


def test_read_sequence(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


def test_pickle(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = unpickled_loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


def test_multiprocess(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
        future = executor.submit(loader.read_sequence, name, contig, start, length)
        sequence = future.result()

    assert_array_equal(sequence, expected_sequence)


def test_cache(assemblies_path: Path) -> None:
    ref = TrackLoader(assemblies_path)
    ref.index(strict=False)

    # Load without cache
    for p in assemblies_path.glob(".track-map-cache-*"):
        p.unlink()
    nocache = TrackLoader(assemblies_path)
    nocache.load()
    assert len(list(assemblies_path.glob(".track-map-cache-*"))) == 1
    assert ref.names == nocache.names
    for name in ref.names:
        assert ref.contigs(name) == nocache.contigs(name)

    # Load with cache
    cache = TrackLoader(assemblies_path)
    cache.load()
    assert len(list(assemblies_path.glob(".track-map-cache-*"))) == 1
    assert ref.names == cache.names
    for name in ref.names:
        assert ref.contigs(name) == cache.contigs(name)

    # Clean up cache
    for p in assemblies_path.glob(".track-map-cache-*"):
        p.unlink()
