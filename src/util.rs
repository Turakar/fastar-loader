use anyhow::{anyhow, Result};
use std::path::Path;

/// Get relative path from root, remove suffix, normalize path separators
pub(crate) fn get_relative_name_without_suffix(
    path: &Path,
    root: &Path,
    suffix: &str,
) -> Result<String> {
    // Get relative path from root
    let relative_path = path
        .strip_prefix(root)
        .map_err(|_| anyhow!("Path is not under root directory"))?;

    // Get filename without suffix
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow!("No file name found"))?
        .to_str()
        .ok_or_else(|| anyhow!("Invalid UTF-8 sequence"))?
        .strip_suffix(suffix)
        .ok_or_else(|| anyhow!("Invalid file name"))?;

    // Get the directory part of the relative path (all subdirectories)
    let parent = relative_path.parent();
    let result = match parent {
        Some(parent) if parent != Path::new("") => {
            // Handle nested subdirectories by joining the full relative directory path with filename
            let normalized_parent = normalize_path_separators(parent)?;
            format!("{}/{}", normalized_parent, filename)
        }
        _ => {
            // File is directly in root directory
            filename.to_string()
        }
    };

    Ok(result)
}

/// Normalize path separators to forward slashes for consistent naming
fn normalize_path_separators(path: &Path) -> Result<String> {
    use std::path::Component;
    let components: Vec<_> = path
        .components()
        .filter_map(|comp| match comp {
            Component::Normal(os_str) => os_str.to_str(),
            _ => None,
        })
        .collect();
    Ok(components.join("/"))
}
