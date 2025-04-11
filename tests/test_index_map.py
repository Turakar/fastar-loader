from pathlib import Path

import pytest  # type: ignore
from fastar_loader import FastarIndex


@pytest.fixture(scope="module")
def index() -> FastarIndex:
    index = FastarIndex("test_data")
    index.build()
    return index


def test_index_names(index: FastarIndex) -> None:
    names = index.names
    assert len(names) == 3
    assert "GCA_000146045.2" in names
    assert "GCF_000182965.3" in names
    assert "GCF_003013715.1" in names


def test_index_get_sequence(
    index: FastarIndex, test_data: tuple[Path, str, str, int, int, bytes]
) -> None:
    _, name, contig, start, length, expected_sequence = test_data
    sequence = index.get_sequence(name, contig, start, length)
    assert sequence == expected_sequence
