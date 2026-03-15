//! Shared symlink helpers for output files.

use tracing::error;

use crate::accumulators::Accumulators;

/// Create/overwrite a symlink (remove-then-create).
#[cfg(unix)]
pub fn symlink_atomic(target: &str, link: &str) {
    let _ = std::fs::remove_file(link);
    if let Err(e) = std::os::unix::fs::symlink(target, link) {
        error!("error symlinking {} to {}: {}", target, link, e);
    }
}

#[cfg(not(unix))]
pub fn symlink_atomic(_target: &str, _link: &str) {
    // Symlinks not supported on this platform
}

/// Create the standard set of accumulator symlinks for a dated day file.
///
/// Given a real file like `LFLE.day.2026-03-15.json`, creates symlinks:
///   - `LFLE.json` (bare fallback, if `include_bare`)
///   - `LFLE.day.json` (latest day)
///   - `LFLE.month.2026-03.json` + `LFLE.month.json`
///   - `LFLE.year.2026.json` + `LFLE.year.json`
///   - `LFLE.yearnz.2025nz.json` + `LFLE.yearnz.json`
pub fn create_accumulator_symlinks(
    dir: &str,
    name: &str,
    ext: &str,
    accumulators: &Accumulators,
    target_filename: &str,
    include_bare: bool,
) {
    if include_bare {
        symlink_atomic(target_filename, &format!("{}/{}.{}", dir, name, ext));
    }
    symlink_atomic(target_filename, &format!("{}/{}.day.{}", dir, name, ext));

    for (acc_name, entry) in [
        ("month", &accumulators.month),
        ("year", &accumulators.year),
        ("yearnz", &accumulators.yearnz),
    ] {
        symlink_atomic(
            target_filename,
            &format!("{}/{}.{}.{}.{}", dir, name, acc_name, entry.file, ext),
        );
        symlink_atomic(
            target_filename,
            &format!("{}/{}.{}.{}", dir, name, acc_name, ext),
        );
    }
}
