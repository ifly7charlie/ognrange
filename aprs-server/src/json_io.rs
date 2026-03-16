//! Shared JSON I/O helpers: atomic writes (plain + gzip) and JSON loading.

use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::error;

/// Write content atomically via a `.working` temp file + rename.
pub fn write_atomic(dir: &str, filename: &str, content: &str) {
    let working = format!("{}/{}.working", dir, filename);
    let final_path = format!("{}/{}", dir, filename);

    if let Err(e) = std::fs::write(&working, content.as_bytes()) {
        error!("Failed to write {}: {}", working, e);
        return;
    }
    if let Err(e) = std::fs::rename(&working, &final_path) {
        error!("Failed to rename {} -> {}: {}", working, final_path, e);
        let _ = std::fs::remove_file(&working);
    }
}

/// Write gzip-compressed content atomically via a `.working` temp file + rename.
pub fn write_gz_atomic(dir: &str, filename: &str, content: &str) {
    let working = format!("{}/{}.working", dir, filename);
    let final_path = format!("{}/{}", dir, filename);

    let result = (|| -> Result<(), String> {
        let file =
            std::fs::File::create(&working).map_err(|e| format!("create {}: {}", working, e))?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder
            .write_all(content.as_bytes())
            .map_err(|e| format!("write {}: {}", working, e))?;
        encoder
            .finish()
            .map_err(|e| format!("finish {}: {}", working, e))?;
        std::fs::rename(&working, &final_path)
            .map_err(|e| format!("rename {} -> {}: {}", working, final_path, e))?;
        Ok(())
    })();

    if let Err(e) = result {
        error!("Failed to write gz: {}", e);
        let _ = std::fs::remove_file(&working);
    }
}

/// Read and parse a JSON file, returning `None` on any error.
pub fn read_json(path: &str) -> Option<serde_json::Value> {
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Try to read JSON from `path`, falling back to `path.gz` (gzip-decompressed).
pub fn read_json_or_gz(path: &str) -> Option<serde_json::Value> {
    if let Some(val) = read_json(path) {
        return Some(val);
    }
    let gz_path = format!("{}.gz", path);
    let bytes = std::fs::read(&gz_path).ok()?;
    let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
    let mut s = String::new();
    std::io::Read::read_to_string(&mut decoder, &mut s).ok()?;
    serde_json::from_str(&s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        write_atomic(dir_path, "test.json", r#"{"hello":"world"}"#);

        let val = read_json(&format!("{}/test.json", dir_path)).unwrap();
        assert_eq!(val["hello"], "world");
    }

    #[test]
    fn test_write_and_read_gz_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        write_gz_atomic(dir_path, "test.json.gz", r#"{"compressed":true}"#);

        // read_json should fail on the .gz file
        assert!(read_json(&format!("{}/test.json.gz", dir_path)).is_none());

        // read_json_or_gz should find the .gz fallback
        let val = read_json_or_gz(&format!("{}/test.json", dir_path)).unwrap();
        assert_eq!(val["compressed"], true);
    }

    #[test]
    fn test_read_json_or_gz_prefers_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        write_atomic(dir_path, "test.json", r#"{"source":"plain"}"#);
        write_gz_atomic(dir_path, "test.json.gz", r#"{"source":"gz"}"#);

        let val = read_json_or_gz(&format!("{}/test.json", dir_path)).unwrap();
        assert_eq!(val["source"], "plain");
    }

    #[test]
    fn test_read_json_missing_file() {
        assert!(read_json("/nonexistent/path.json").is_none());
    }

    #[test]
    fn test_read_json_or_gz_missing_both() {
        assert!(read_json_or_gz("/nonexistent/path.json").is_none());
    }
}
