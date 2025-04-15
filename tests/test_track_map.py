import multiprocessing
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest
from fastar_loader import TrackLoader


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


def test_read_sequence(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, list[float]]
) -> None:
    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence


def test_pickle(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, list[float]]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = unpickled_loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence


def test_multiprocess(
    loader: TrackLoader, track_test_data: tuple[Path, str, str, int, int, list[float]]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = track_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
        future = executor.submit(loader.read_sequence, name, contig, start, length)
        sequence = future.result()

    assert sequence == expected_sequence
