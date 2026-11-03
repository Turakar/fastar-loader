use anyhow::{Context, Result};
use rkyv::Archived;

/// Positions of `entries`, ordered by name, so that a contig can be found by binary search
/// instead of a linear scan (which made reading every contig of an assembly with millions of
/// contigs quadratic). The sort is stable: of several entries with the same name, the first in
/// file order comes first, as with the linear scan.
pub(super) fn positions_by_name<T>(entries: &[T], name: impl Fn(&T) -> &[u8]) -> Result<Vec<u32>> {
    let n = u32::try_from(entries.len()).context("Too many contigs for the name index")?;
    let mut positions: Vec<u32> = (0..n).collect();
    positions.sort_by(|&a, &b| name(&entries[a as usize]).cmp(name(&entries[b as usize])));
    Ok(positions)
}

/// The first entry (in file order) called `name`, by binary search in `positions`
/// (as built by [`positions_by_name`]).
pub(super) fn find_by_name<'a, T>(
    entries: &'a [T],
    positions: &[Archived<u32>],
    name: &[u8],
    entry_name: impl Fn(&T) -> &[u8],
) -> Option<&'a T> {
    let i = positions.partition_point(|p| entry_name(&entries[p.to_native() as usize]) < name);
    positions
        .get(i)
        .map(|p| &entries[p.to_native() as usize])
        .filter(|entry| entry_name(entry) == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::rancor;

    #[test]
    fn test_find_by_name() {
        let names: Vec<&[u8]> = vec![b"chr2", b"chr10", b"chr1", b"chr2", b"scaffold_7"];
        let positions = positions_by_name(&names, |n| *n).unwrap();
        let bytes = rkyv::to_bytes::<rancor::Error>(&positions).unwrap();
        let archived = rkyv::access::<Archived<Vec<u32>>, rancor::Error>(&bytes).unwrap();
        for name in &names {
            let found = find_by_name(&names, archived, name, |n| *n).unwrap();
            // the first entry with that name in file order, as the linear scan found
            let first = names.iter().position(|n| n == name).unwrap();
            assert!(std::ptr::eq(found, &names[first]));
        }
        for missing in [&b""[..], b"chr3", b"chr", b"zzz"] {
            assert!(find_by_name(&names, archived, missing, |n| *n).is_none());
        }
    }
}
