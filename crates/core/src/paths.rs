//! Turning sender-supplied names into safe paths on the receiver's disk.
//!
//! The sender controls file names, so they're untrusted. Every incoming path is
//! normalised to a relative path made of plain components. Anything that could
//! escape the download folder, hit a Windows device name, or create an
//! alternate data stream is rejected or rewritten.

use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

const MAX_COMPONENT_BYTES: usize = 200;
const MAX_DEPTH: usize = 32;

const WINDOWS_RESERVED: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9", "CONIN$",
    "CONOUT$",
];

/// Convert a sender-supplied relative path (`/` or `\` separated) into a safe
/// relative [`PathBuf`].
///
/// Rejects: absolute paths, `..`, drive letters, `:` (alternate data streams),
/// control characters, empty names, excessive depth.
/// Rewrites: characters Windows forbids (`<>"|?*`) become `_`; trailing dots
/// and spaces are trimmed; reserved device names get a `_` prefix.
pub fn sanitize_relative_path(raw: &str) -> Result<PathBuf> {
    let bad = |why: &str| Error::UnsafePath(format!("{raw:?}: {why}"));

    if raw.is_empty() {
        return Err(bad("empty"));
    }
    if raw.starts_with('/') || raw.starts_with('\\') {
        return Err(bad("absolute path"));
    }
    if raw.chars().any(|c| c.is_control()) {
        return Err(bad("control character"));
    }
    if raw.contains(':') {
        return Err(bad("':' is not allowed (drive letter or stream)"));
    }

    let mut out = PathBuf::new();
    let mut depth = 0;
    for comp in raw.split(['/', '\\']) {
        if comp.is_empty() || comp == "." {
            continue;
        }
        if comp == ".." {
            return Err(bad("'..' is not allowed"));
        }
        let clean = sanitize_component(comp).ok_or_else(|| bad("empty component"))?;
        out.push(clean);
        depth += 1;
        if depth > MAX_DEPTH {
            return Err(bad("too deeply nested"));
        }
    }
    if depth == 0 {
        return Err(bad("no file name"));
    }
    Ok(out)
}

fn sanitize_component(comp: &str) -> Option<String> {
    let replaced: String = comp
        .chars()
        .map(|c| match c {
            '<' | '>' | '"' | '|' | '?' | '*' => '_',
            c => c,
        })
        .collect();
    // Windows silently strips trailing dots and spaces; do it explicitly so
    // "a." and "a" don't collide unexpectedly.
    let trimmed = replaced.trim_end_matches(['.', ' ']).trim_start();
    if trimmed.is_empty() {
        return None;
    }
    let stem = trimmed.split('.').next().unwrap_or("").to_ascii_uppercase();
    let mut s = if WINDOWS_RESERVED.contains(&stem.as_str()) {
        format!("_{trimmed}")
    } else {
        trimmed.to_string()
    };
    if s.len() > MAX_COMPONENT_BYTES {
        s = truncate_keep_extension(&s, MAX_COMPONENT_BYTES);
    }
    Some(s)
}

/// Truncate a file name to at most `max` bytes, keeping its extension and
/// respecting UTF-8 boundaries.
fn truncate_keep_extension(name: &str, max: usize) -> String {
    let (stem, ext) = match name.rfind('.') {
        Some(i) if i > 0 && name.len() - i <= 16 => (&name[..i], &name[i..]),
        _ => (name, ""),
    };
    let budget = max.saturating_sub(ext.len());
    let mut end = budget.min(stem.len());
    while !stem.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &stem[..end], ext)
}

/// Find a path that doesn't exist yet: `name.ext`, `name (1).ext`, ...
pub fn unique_path(desired: &Path) -> PathBuf {
    if !desired.exists() {
        return desired.to_path_buf();
    }
    let parent = desired.parent().unwrap_or_else(|| Path::new(""));
    let file_name = desired
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let (stem, ext) = match file_name.rfind('.') {
        Some(i) if i > 0 => (&file_name[..i], &file_name[i..]),
        _ => (file_name.as_str(), ""),
    };
    for n in 1..10_000 {
        let candidate = parent.join(format!("{stem} ({n}){ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    // Practically unreachable; fall back to a random suffix.
    parent.join(format!("{stem} ({}){ext}", crate::util::random_token(6)))
}

/// Join a sanitized relative path onto `root` and double-check it stays inside.
pub fn safe_join(root: &Path, rel: &Path) -> Result<PathBuf> {
    use std::path::Component;
    for c in rel.components() {
        if !matches!(c, Component::Normal(_)) {
            return Err(Error::UnsafePath(rel.display().to_string()));
        }
    }
    Ok(root.join(rel))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> String {
        sanitize_relative_path(s)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/")
    }

    #[test]
    fn plain_names_pass_through() {
        assert_eq!(ok("photo.jpg"), "photo.jpg");
        assert_eq!(ok("folder/sub/file.txt"), "folder/sub/file.txt");
        assert_eq!(ok("folder\\file.txt"), "folder/file.txt");
        assert_eq!(ok("./a/./b.txt"), "a/b.txt");
        assert_eq!(ok("a//b.txt"), "a/b.txt");
        assert_eq!(ok("résumé 2026.pdf"), "résumé 2026.pdf");
    }

    #[test]
    fn traversal_is_rejected() {
        for p in [
            "../x",
            "a/../../x",
            "..",
            "a/..",
            "..\\x",
            "/etc/passwd",
            "\\\\server\\share",
            "C:\\Windows\\x",
            "c:x",
            "file.txt:stream",
            "",
            "/",
            ".",
            "a\u{0}b",
            "a\nb",
        ] {
            assert!(sanitize_relative_path(p).is_err(), "should reject {p:?}");
        }
    }

    #[test]
    fn windows_quirks_are_rewritten() {
        assert_eq!(ok("CON"), "_CON");
        assert_eq!(ok("con.txt"), "_con.txt");
        assert_eq!(ok("dir/LPT1.log"), "dir/_LPT1.log");
        assert_eq!(ok("what?.txt"), "what_.txt");
        assert_eq!(ok("a<b>c|d*e\"f"), "a_b_c_d_e_f");
        assert_eq!(ok("trailing. . "), "trailing");
        assert!(sanitize_relative_path("...").is_err());
        assert!(sanitize_relative_path("   ").is_err());
    }

    #[test]
    fn long_names_are_truncated_keeping_extension() {
        let long = format!("{}.jpeg", "x".repeat(400));
        let p = ok(&long);
        assert!(p.len() <= MAX_COMPONENT_BYTES);
        assert!(p.ends_with(".jpeg"));
        let emoji = format!("{}.txt", "😀".repeat(100));
        let p = ok(&emoji);
        assert!(p.len() <= MAX_COMPONENT_BYTES && p.ends_with(".txt"));
    }

    #[test]
    fn depth_is_limited() {
        let deep = vec!["d"; MAX_DEPTH + 1].join("/");
        assert!(sanitize_relative_path(&deep).is_err());
    }

    #[test]
    fn sanitized_paths_never_escape() {
        // Cheap fuzz: random byte soup from a hostile alphabet.
        let alphabet = ['a', '.', '/', '\\', ':', ' ', '?', 'C', 'O', 'N', '~', '$'];
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        for _ in 0..20_000 {
            let len = (seed % 12) as usize + 1;
            let mut s = String::new();
            for _ in 0..len {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                s.push(alphabet[(seed % alphabet.len() as u64) as usize]);
            }
            if let Ok(p) = sanitize_relative_path(&s) {
                assert!(p.is_relative(), "{s:?} -> {p:?}");
                for c in p.components() {
                    assert!(
                        matches!(c, std::path::Component::Normal(_)),
                        "{s:?} -> {p:?}"
                    );
                }
                assert!(safe_join(Path::new("/root"), &p).is_ok());
            }
        }
    }

    #[test]
    fn unique_path_appends_counter() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.txt");
        assert_eq!(unique_path(&p), p);
        std::fs::write(&p, b"x").unwrap();
        assert_eq!(unique_path(&p), dir.path().join("a (1).txt"));
        std::fs::write(dir.path().join("a (1).txt"), b"x").unwrap();
        assert_eq!(unique_path(&p), dir.path().join("a (2).txt"));
        let d = dir.path().join("folder");
        std::fs::create_dir(&d).unwrap();
        assert_eq!(unique_path(&d), dir.path().join("folder (1)"));
    }
}
