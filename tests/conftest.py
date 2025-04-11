import gzip
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pyfaidx
import pytest


@contextmanager
def pyfaidx_fasta(path: Path) -> Iterator[pyfaidx.Fasta]:
    with tempfile.TemporaryDirectory(prefix="fastar-loader-tests") as tmpdir:
        uncompressed_path = Path(tmpdir) / "assembly.fna"
        with open(uncompressed_path, "wb") as f, gzip.open(path, "rb") as gz:
            shutil.copyfileobj(gz, f)
        with pyfaidx.Fasta(uncompressed_path) as fasta:
            yield fasta


@pytest.fixture(
    params=[
        dict(name="GCA_000146045.2", contig="BK006935.2", start=0, length=60),
        dict(name="GCA_000146045.2", contig="BK006949.2", start=200000, length=60),
        dict(name="GCF_000182965.3", contig="NC_032094.1", start=1032000, length=1033292 - 1032000),
        dict(name="GCF_003013715.1", contig="NC_072815.1", start=10000, length=10000),
    ],
    scope="session",
)
def test_data(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[Path, str, str, int, int, bytes]]:
    param = request.param
    name = param["name"]
    contig = param["contig"]
    start = param["start"]
    length = param["length"]
    path = Path("test_data") / f"{name}.fna.gz"
    with pyfaidx_fasta(path) as fasta:
        sequence = fasta[contig][start : start + length]
        assert sequence is not None
        assert len(sequence) == length
        yield (
            path,
            name,
            contig,
            start,
            length,
            sequence.seq.encode("utf-8"),
        )
