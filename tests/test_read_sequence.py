from pathlib import Path

import numpy as np
from fastar_loader import read_sequence  # type: ignore
from numpy.testing import assert_array_equal


def test_read_sequence(fasta_test_data: tuple[Path, str, str, int, int, np.ndarray]) -> None:
    path, _, contig, start, length, sequence = fasta_test_data
    rust_sequence = read_sequence(
        str(path), contig, start, length, gzi_path=f"{path}.gzi", fai_path=f"{path}.fai"
    )
    assert_array_equal(rust_sequence, sequence)


def test_read_sequence_implicit_index(
    fasta_test_data: tuple[Path, str, str, int, int, np.ndarray],
) -> None:
    path, _, contig, start, length, sequence = fasta_test_data
    rust_sequence = read_sequence(str(path), contig, start, length)
    assert_array_equal(rust_sequence, sequence)
