from pathlib import Path

from . import fastar_loader as _rust


def read_sequence(
    fasta_path: str | Path,
    contig: str,
    start: int,
    length: int,
    gzi_path: str | Path | None = None,
    fai_path: str | Path | None = None,
) -> bytes:
    fasta_path = str(fasta_path)
    if gzi_path is None:
        gzi_path = f"{fasta_path}.gzi"
    else:
        gzi_path = str(gzi_path)
    if fai_path is None:
        fai_path = f"{fasta_path}.fai"
    else:
        fai_path = str(fai_path)
    return _rust.read_sequence(fasta_path, gzi_path, fai_path, contig, start, length)


class FastarIndex:
    def __init__(self, path: str | Path):
        self._path = str(path)
        self._index_map_ = None

    def build(self) -> None:
        self._index_map_ = _rust.IndexMap.build(self._path)

    @property
    def _index_map(self) -> _rust.IndexMap:
        if self._index_map_ is None:
            raise ValueError("Index map not built. Call build() first.")
        return self._index_map_

    @property
    def names(self) -> list[str]:
        return self._index_map.names

    def get_sequence(self, name: str, contig: str, start: int, length: int) -> bytes:
        return self._index_map.get_sequence(name, contig, start, length)
