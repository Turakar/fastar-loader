import multiprocessing
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest
from fastar_loader import FastarLoader


@pytest.fixture()
def loader(assemblies_path: Path) -> FastarLoader:
    loader = FastarLoader(assemblies_path)
    loader.index()
    return loader


def test_names(loader: FastarLoader, expected_names: list[str]) -> None:
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_names_shmem(loader: FastarLoader, expected_names: list[str]) -> None:
    loader.to_shared_memory()
    names = loader.names
    assert len(names) == len(expected_names)
    assert all(name in names for name in expected_names)


def test_read_sequence(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence


def test_pickle(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence

    pickled_loader = pickle.dumps(loader)
    unpickled_loader = pickle.loads(pickled_loader)

    sequence = unpickled_loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence


def test_multiprocess(
    loader: FastarLoader, fasta_test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    loader.to_shared_memory()

    _, name, contig, start, length, expected_sequence = fasta_test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
        future = executor.submit(loader.read_sequence, name, contig, start, length)
        sequence = future.result()

    assert sequence == expected_sequence
