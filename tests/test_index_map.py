import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest
from fastar_loader import FastarLoader


@pytest.fixture()
def loader() -> FastarLoader:
    loader = FastarLoader("test_data")
    loader.index()
    return loader


def test_names(loader: FastarLoader) -> None:
    names = loader.names
    assert len(names) == 3
    assert "GCA_000146045.2" in names
    assert "GCF_000182965.3" in names
    assert "GCF_003013715.1" in names


def test_names_shmem(loader: FastarLoader) -> None:
    loader.to_shared_memory()
    names = loader.names
    assert len(names) == 3
    assert "GCA_000146045.2" in names
    assert "GCF_000182965.3" in names
    assert "GCF_003013715.1" in names


def test_read_sequence(
    loader: FastarLoader, test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    _, name, contig, start, length, expected_sequence = test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence


def test_pickle(loader: FastarLoader, test_data: tuple[Path, str, str, int, int, bytes]) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = unpickled_loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence


def test_multiprocess(
    loader: FastarLoader, test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence

    with ProcessPoolExecutor() as executor:
        future = executor.submit(loader.read_sequence, name, contig, start, length)
        sequence = future.result()

    assert sequence == expected_sequence
