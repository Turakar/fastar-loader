use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

pub(super) fn with_suffix(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p.into_os_string();
    p.push(s);
    p.into()
}

pub(super) fn get_name_without_suffix(path: &Path, suffix: &str) -> Result<String> {
    let name = path
        .file_name()
        .ok_or_else(|| anyhow!("No file name found"))?
        .to_str()
        .ok_or_else(|| anyhow!("Invalid UTF-8 sequence"))?
        .strip_suffix(suffix)
        .ok_or_else(|| anyhow!("Invalid file name"))?
        .to_string();
    Ok(name)
}
