from pathlib import Path

import numpy as np

from . import fastar_loader as _rust  # type: ignore


def read_sequence(
    fasta_path: str | Path,
    contig: str,
    start: int,
    length: int,
    gzi_path: str | Path | None = None,
    fai_path: str | Path | None = None,
) -> np.ndarray:
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


class FastarLoader:
    def __init__(
        self,
        path: str | Path,
        strict: bool = True,
        force_build: bool = False,
        no_cache: bool = False,
        min_contig_length: int = 0,
        num_workers: int | None = None,
    ):
        self._path = str(path)
        self._index_map = _rust.FastaMap.load(
            self._path, strict, force_build, no_cache, min_contig_length, num_workers
        )

    @property
    def names(self) -> list[str]:
        return self._index_map.names

    def contigs(self, name: str) -> list[tuple[str, int]]:
        return [
            (contig.decode("utf-8"), length) for contig, length in self._index_map.contigs(name)
        ]

    def read_sequence(self, name: str, contig: str, start: int, length: int) -> bytes:
        return self._index_map.read_sequence(name, contig.encode(), start, length)

    def __getstate__(self) -> dict[str, object]:
        d = self.__dict__.copy()
        d["_index_map"] = self._index_map.handle
        return d

    def __setstate__(self, state: dict[str, object]) -> None:
        state["_index_map"] = _rust.FastaMap.from_handle(state["_index_map"])
        self.__dict__.update(state)


class TrackLoader:
    def __init__(
        self,
        path: str | Path,
        strict: bool = True,
        force_build: bool = False,
        no_cache: bool = False,
        min_contig_length: int = 0,
        num_workers: int | None = None,
    ):
        self._path = str(path)
        self._index_map = _rust.TrackMap.load(
            self._path, strict, force_build, no_cache, min_contig_length, num_workers
        )

    @property
    def names(self) -> list[str]:
        return self._index_map.names

    def contigs(self, name: str) -> list[tuple[str, int]]:
        return [
            (contig.decode("utf-8"), length) for contig, length in self._index_map.contigs(name)
        ]

    def read_sequence(self, name: str, contig: str, start: int, length: int) -> np.ndarray:
        return self._index_map.read_sequence(name, contig.encode(), start, length)

    def __getstate__(self) -> dict[str, object]:
        d = self.__dict__.copy()
        d["_index_map"] = self._index_map.handle
        return d

    def __setstate__(self, state: dict[str, object]) -> None:
        state["_index_map"] = _rust.TrackMap.from_handle(state["_index_map"])
        self.__dict__.update(state)
