from pathlib import Path

import pytest  # type: ignore
from fastar_loader import FastarLoader


@pytest.fixture(scope="module")
def loader() -> FastarLoader:
    loader = FastarLoader("test_data")
    loader.index()
    return loader


def test_index_names(loader: FastarLoader) -> None:
    names = loader.names
    assert len(names) == 3
    assert "GCA_000146045.2" in names
    assert "GCF_000182965.3" in names
    assert "GCF_003013715.1" in names


def test_index_names_shmem() -> None:
    loader = FastarLoader("test_data")
    loader.index()
    loader.to_shared_memory()
    names = loader.names
    assert len(names) == 3
    assert "GCA_000146045.2" in names
    assert "GCF_000182965.3" in names
    assert "GCF_003013715.1" in names


def test_index_read_sequence(
    loader: FastarLoader, test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    _, name, contig, start, length, expected_sequence = test_data
    sequence = loader.read_sequence(name, contig, start, length)
    assert sequence == expected_sequence
