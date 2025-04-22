.. fastar-loader documentation master file, created by
   sphinx-quickstart on Tue Apr 22 09:31:26 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

fastar-loader documentation
===========================

A faster loader for compressed FASTA files using indices in shared memory.

If you want to read a part of a sequence from a compressed FASTA file, you typically want to use the blocked GZIP format (BGZF) with a `.gzi` BGZF index and a `.fai` FASTA index. This requires you to keep the indices in memory, which can become an issue if you do this in a multiprocessing environment, because you will have one copy of each index for each process (e.g., in a Torch DataLoader).

To alleviate this, this library can transfer the indices to shared memory, allowing for usage of the indices from multiple processes while keeping only one copy of the indices in memory.

Input
-----
All files read by the index are expected to be in one directory and should follow the following naming scheme (where `XXX` is referred to as the name of a FASTA):

- ``XXX.fna.gz``: The BGZF-compressed FASTA file
- ``XXX.fna.gz.gzi``: The BGZF index
- ``XXX.fna.gz.fai``: The FASTA index file (faidx).

To create these files, you can use the following commands::

    $ bgzip XXX.fna
    $ samtools faidx XXX.fna

Where `bgzip` is from HTSlib and `samtools` from SAMtools.


Example
-------
Creating a loader and loading a certain region::

    from fastar_loader import FastarLoader
    loader = FastarLoader("test_data")
    loader.read_sequence(name="GCA_000146045.2.fna.gz", contig="BK006935.2", start=0, length=60)


After the first load, the indices are cached to disk in the same directory for faster loading.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules.rst
