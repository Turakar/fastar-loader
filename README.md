# fastar-loader

![](https://github.com/Turakar/fastar-loader/workflows/docs/badge.svg)

A faster loader for compressed FASTA files using indices in shared memory.

If you want to read a part of a sequence from a compressed FASTA file, you typically want to use the blocked GZIP format (BGZF) with a `.gzi` BGZF index and a `.fai` FASTA index. This requires you to keep the indices in memory, which can become an issue if you do this in a multiprocessing environment, because you will have one copy of each index for each process (e.g., in a Torch DataLoader).

To alleviate this, this library can transfer the indices to shared memory, allowing for usage of the indices from multiple processes while keeping only one copy of the indices in memory.


## Input
All files read by the index are expected to be in one directory and should follow the following naming scheme (where `XXX` is referred to as the name of a FASTA):

- `XXX.fna.gz`: The BGZF-compressed FASTA file
- `XXX.fna.gz.gzi`: The BGZF index
- `XXX.fna.gz.fai`: The FASTA index file (faidx).

To create these files, you can use the following commands:

```
$ bgzip XXX.fna
$ samtools faidx XXX.fna
```

Where `bgzip` is from HTSlib and `samtools` from SAMtools.


## Example
```python
from fastar_loader import FastarLoader
loader = FastarLoader("test_data")
loader.read_sequence(name="GCA_000146045.2.fna.gz", contig="BK006935.2", start=0, length=60)
```

After the first load, the indices are cached to disk in the same directory for faster loading.


## Development
This project uses uv, maturin, pytest, cargo, git-lfs and pre-commit. Useful commands include:
- `uv sync`: Creates the virtual environment and installs the Rust library.
- `maturin develop`: Installs a development version of the Rust library (faster).
- `pytest`: Runs the Python tests.
- `cargo test`: Runs the Rust tests.
- `pre-commit install`: Run this **before your first commit** to ensure that all checks are run on each commit.


## Implementation details
Storing the indices to shared memory is not straightforward. Most importantly, som Rust types like `Vec` do not allocate their data on the stack, but on the heap, which breaks a naive memcopy. Thus, this library uses `rkyv` to create an archived version of the indices which allows for storing the whole index in one contiguous slice of memory, which can then be transferred to shared memory and read from there. This unfortunately requires duplication of the indexing logic from `noodles` for the newly created `IndexMap` and `ArchivedIndexMap` types.
