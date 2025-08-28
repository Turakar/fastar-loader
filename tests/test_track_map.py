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
    loader = TrackLoader(tracks_path, no_cache=True)
    return loader


def test_names(loader: TrackLoader, expected_names: list[str]) -> None:
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_structure(loader: TrackLoader, track_structure: dict[str, list[tuple[str, int]]]) -> None:
    for name, contigs in track_structure.items():
        assert loader.contigs(name) == contigs


def test_read_sequence(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = _read_f32(loader, name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


def test_pickle(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = _read_f32(loader, name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = _read_f32(unpickled_loader, name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)


def test_multiprocess(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, np.ndarray]
) -> None:
    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = _read_f32(loader, name, contig, start, length)
    assert_array_equal(sequence, expected_sequence)

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
        future = executor.submit(_read_f32, loader, name, contig, start, length)
        sequence = future.result()
    assert_array_equal(sequence, expected_sequence)


def test_cache(tracks_path: Path) -> None:
    ref = TrackLoader(tracks_path, no_cache=True)

    # Load without cache
    clean_cache(tracks_path)
    nocache = TrackLoader(tracks_path)
    assert len(list(tracks_path.glob(".track-map-cache-*"))) == 1
    assert ref.names == nocache.names
    for name in ref.names:
        assert ref.contigs(name) == nocache.contigs(name)

    # Load with cache
    cache = TrackLoader(tracks_path)
    assert ref.names == cache.names
    for name in ref.names:
        assert ref.contigs(name) == cache.contigs(name)

    # Clean up cache
    clean_cache(tracks_path)


def clean_cache(assemblies_path: Path) -> None:
    for cache_file in assemblies_path.glob(".track-map-cache-*"):
        cache_file.unlink(missing_ok=True)


def test_min_contig_length(tracks_path: Path, expected_names: list[str]) -> None:
    min_length = 1_000_000
    ref = TrackLoader(tracks_path, no_cache=True)
    restricted = TrackLoader(tracks_path, min_contig_length=min_length, no_cache=True)
    print(ref.names)
    print(restricted.names)
    for name in expected_names:
        ref_contigs = ref.contigs(name)
        print(ref_contigs)
        restricted_contigs = restricted.contigs(name)
        print(restricted_contigs)
        for contig, length in ref_contigs:
            if length >= min_length:
                assert (contig, length) in restricted_contigs
            else:
                assert (contig, length) not in restricted_contigs
        for contig, length in restricted_contigs:
            assert (contig, length) in ref_contigs


def _read_f32(
    track_loader: TrackLoader, name: str, contig: str, start: int, length: int
) -> np.ndarray:
    bytes_data = track_loader.read_sequence(name, contig, start * 4, length * 4)
    return np.frombuffer(bytes_data, dtype=np.float32)
