from pathlib import Path

from fastar_loader import read_sequence  # type: ignore


def test_read_sequence(fasta_test_data: tuple[Path, str, str, int, int, bytes]) -> None:
    path, _, contig, start, length, sequence_bytes = fasta_test_data
    rust_sequence = read_sequence(
        str(path), contig, start, length, gzi_path=f"{path}.gzi", fai_path=f"{path}.fai"
    )
    assert rust_sequence == sequence_bytes


def test_read_sequence_implicit_index(
    fasta_test_data: tuple[Path, str, str, int, int, bytes],
) -> None:
    path, _, contig, start, length, sequence_bytes = fasta_test_data
    rust_sequence = read_sequence(str(path), contig, start, length)
    assert rust_sequence == sequence_bytes
