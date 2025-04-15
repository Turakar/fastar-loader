import gzip
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import numpy as np
import polars as pl
import pyfaidx
import pytest

_TEST_REGIONS = [
    dict(name="GCA_000146045.2", contig="BK006935.2", start=0, length=60),
    dict(name="GCA_000146045.2", contig="BK006949.2", start=200000, length=60),
    dict(name="GCF_000182965.3", contig="NC_032094.1", start=1032000, length=1033292 - 1032000),
    dict(name="GCF_003013715.1", contig="NC_072815.1", start=10000, length=10000),
]


@pytest.fixture(scope="session")
def expected_names() -> list[str]:
    return [
        "GCA_000146045.2",
        "GCF_000182965.3",
        "GCF_003013715.1",
    ]


@pytest.fixture(scope="session")
def assemblies_path() -> Path:
    return Path("test-data") / "assemblies"


@pytest.fixture(scope="session")
def tracks_path() -> Path:
    return Path("test-data") / "tracks"


@contextmanager
def pyfaidx_fasta(path: Path) -> Iterator[pyfaidx.Fasta]:
    with tempfile.TemporaryDirectory(prefix="fastar-loader-tests-") as tmpdir:
        uncompressed_path = Path(tmpdir) / "assembly.fna"
        with open(uncompressed_path, "wb") as f, gzip.open(path, "rb") as gz:
            shutil.copyfileobj(gz, f)
        with pyfaidx.Fasta(uncompressed_path) as fasta:
            yield fasta


@pytest.fixture(
    params=_TEST_REGIONS,
    scope="session",
)
def fasta_test_data(
    request: pytest.FixtureRequest,
    assemblies_path: Path,
) -> tuple[Path, str, str, int, int, np.ndarray]:
    param = request.param
    name = param["name"]
    contig = param["contig"]
    start = param["start"]
    length = param["length"]
    path = assemblies_path / f"{name}.fna.gz"
    with pyfaidx_fasta(path) as fasta:
        record = fasta[contig][start : start + length]
        assert record is not None
        sequence = np.frombuffer(record.seq.encode("utf-8"), dtype=np.uint8)
    assert sequence is not None
    assert len(sequence) == length
    return (
        path,
        name,
        contig,
        start,
        length,
        sequence,
    )


@pytest.fixture(
    params=_TEST_REGIONS,
    scope="session",
)
def track_test_data(
    request: pytest.FixtureRequest,
    tracks_path: Path,
) -> tuple[Path, str, str, int, int, np.ndarray]:
    # parse request
    param = request.param
    name = param["name"]
    contig = param["contig"]
    start = param["start"]
    length = param["length"]
    path = tracks_path / f"{name}.track.gz"

    # find region
    index = pl.read_csv(
        f"{path}.idx",
        separator="\t",
        has_header=False,
        new_columns=["contig", "offset"],
    )
    row = index.filter(pl.col("contig") == contig).select(pl.col("offset"))
    assert len(row) > 0
    offset = row.item() + start

    # read data
    with tempfile.TemporaryDirectory(prefix="fastar-loader-tests-") as tmpdir:
        uncompressed_path = Path(tmpdir) / "track"
        with open(uncompressed_path, "wb") as f, gzip.open(path, "rb") as gz:
            shutil.copyfileobj(gz, f)
        mmap = np.memmap(uncompressed_path, dtype=np.float32, mode="r")
        assert mmap.shape[0] == index["offset"].last()
        assert offset + length <= mmap.shape[0]
        data = mmap[offset : offset + length]

    return (
        path,
        name,
        contig,
        start,
        length,
        data,
    )
