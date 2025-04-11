use std::path::PathBuf;

pub(super) fn with_suffix(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p.into_os_string();
    p.push(s);
    p.into()
}
