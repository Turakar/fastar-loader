from pathlib import Path

from . import fastar_loader as _rust  # type: ignore


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


class FastarLoader:
    def __init__(self, path: str | Path):
        self._path = str(path)
        self._index_map_ = None

    def index(self) -> None:
        self._index_map_ = _rust.FastaMap.build(self._path)

    @property
    def _index_map(self) -> _rust.FastaMap:
        if self._index_map_ is None:
            raise ValueError("Index map not built. Call index() first.")
        return self._index_map_

    @property
    def in_shared_memory(self) -> bool:
        return isinstance(self._index_map_, _rust.ShmemFastaMap)

    @property
    def names(self) -> list[str]:
        return self._index_map.names

    def read_sequence(self, name: str, contig: str, start: int, length: int) -> bytes:
        return self._index_map.read_sequence(name, contig.encode(), start, length)

    def to_shared_memory(self) -> None:
        if self.in_shared_memory:
            raise ValueError("Index map already in shared memory.")
        self._index_map_ = self._index_map.to_shared_memory()

    def __getstate__(self) -> dict[str, object]:
        if not self.in_shared_memory:
            raise ValueError("Index map can only be pickled if it is in shared memory.")
        d = self.__dict__.copy()
        d["_index_map_"] = self._index_map.handle
        return d

    def __setstate__(self, state: dict[str, object]) -> None:
        if "_index_map_" in state:
            state["_index_map_"] = _rust.ShmemFastaMap.from_handle(state["_index_map_"])
        self.__dict__.update(state)

    def __repr__(self):
        if self._index_map_ is None:
            state = "uninitialized"
        elif self.in_shared_memory:
            state = "shared_memory"
        else:
            state = "local_memory"
        return f"FastarLoader(path={self._path}, state={state})"
