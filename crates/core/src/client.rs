//! The send side.

use crate::error::{Error, Result};
use crate::peers::Peer;
use crate::protocol::*;
use bytes::Bytes;
use futures_util::{stream, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;
pub use tokio_util::sync::CancellationToken;

const READ_CHUNK: usize = 256 * 1024;
/// Files uploaded in parallel. Helps with many small files; for one big file it
/// makes no difference.
const PARALLEL_UPLOADS: usize = 3;
/// Retries per file on network errors.
const UPLOAD_RETRIES: usize = 2;

/// HTTP client for LAN traffic. Proxies are disabled: system or environment
/// proxies would route LAN requests off the LAN.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(4))
        .tcp_nodelay(true)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .expect("http client")
}

fn url(addr: SocketAddr, path: &str) -> String {
    // SocketAddr's Display brackets IPv6 for us.
    format!("http://{addr}{API_PREFIX}{path}")
}

/// Turn a non-2xx response into an error.
async fn check(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let msg = resp
        .json::<ErrorBody>()
        .await
        .map(|b| b.error)
        .unwrap_or_else(|_| status.to_string());
    Err(match status.as_u16() {
        403 => Error::Declined,
        409 => Error::Busy,
        410 => Error::Cancelled,
        422 => Error::ChecksumMismatch(msg),
        503 => Error::NotReceiving,
        _ => Error::Protocol(format!("{status}: {msg}")),
    })
}

/// `GET /info` with a short timeout.
pub async fn info(addr: SocketAddr) -> Result<DeviceInfo> {
    let resp = http_client()
        .get(url(addr, "/info"))
        .timeout(Duration::from_secs(4))
        .send()
        .await?;
    let info: DeviceInfo = check(resp).await?.json().await?;
    if info.protocol != PROTOCOL_VERSION {
        return Err(Error::Protocol(format!(
            "{} speaks protocol {}, we speak {}",
            info.alias, info.protocol, PROTOCOL_VERSION
        )));
    }
    Ok(info)
}

/// `POST /register`: tell a device about us; it replies with its own info.
pub async fn register(addr: SocketAddr, me: &DeviceInfo) -> Result<DeviceInfo> {
    let resp = http_client()
        .post(url(addr, "/register"))
        .timeout(Duration::from_secs(4))
        .json(me)
        .send()
        .await?;
    Ok(check(resp).await?.json().await?)
}

/// Find a working address for a discovered peer, checking it's really them.
pub async fn connect(peer: &Peer) -> Result<SocketAddr> {
    let mut last_err = None;
    for addr in peer.endpoints() {
        match info(addr).await {
            Ok(i) if i.id == peer.info.id => return Ok(addr),
            Ok(i) => {
                last_err = Some(Error::IdentityMismatch {
                    expected: peer.info.short_id().to_string(),
                    actual: i.short_id().to_string(),
                })
            }
            Err(e) => last_err = Some(e),
        }
    }
    tracing::debug!("connect {}: {:?}", peer.info.alias, last_err);
    Err(Error::Unreachable(peer.info.alias.clone()))
}

/// A local file queued for sending.
#[derive(Debug, Clone)]
pub struct SourceFile {
    pub id: String,
    pub abs: PathBuf,
    /// Path the receiver will see, `/`-separated.
    pub rel: String,
    pub size: u64,
}

/// Expand the user's selection (files and folders) into a flat file list.
///
/// Folders keep their name as the top-level directory, so sending `Photos/`
/// arrives as `Photos/...`. Symlinks and special files are skipped.
pub fn collect_files(paths: &[PathBuf]) -> Result<Vec<SourceFile>> {
    let mut out = Vec::new();
    for p in paths {
        let meta =
            std::fs::metadata(p).map_err(|e| Error::Other(format!("{}: {e}", p.display())))?;
        if meta.is_file() {
            let name = file_name(p)?;
            out.push(SourceFile {
                id: String::new(),
                abs: p.clone(),
                rel: name,
                size: meta.len(),
            });
        } else if meta.is_dir() {
            let root_name = file_name(p)?;
            for entry in walkdir::WalkDir::new(p)
                .follow_links(false)
                .sort_by_file_name()
            {
                let entry = entry.map_err(|e| Error::Other(e.to_string()))?;
                if !entry.file_type().is_file() {
                    continue;
                }
                let rel_inner = entry
                    .path()
                    .strip_prefix(p)
                    .map_err(|e| Error::Other(e.to_string()))?;
                let mut rel = root_name.clone();
                for c in rel_inner.components() {
                    rel.push('/');
                    rel.push_str(&c.as_os_str().to_string_lossy());
                }
                let size = entry
                    .metadata()
                    .map_err(|e| Error::Other(e.to_string()))?
                    .len();
                out.push(SourceFile {
                    id: String::new(),
                    abs: entry.into_path(),
                    rel,
                    size,
                });
            }
        }
    }
    for (i, f) in out.iter_mut().enumerate() {
        f.id = format!("f{i}");
    }
    if out.is_empty() {
        return Err(Error::Other("nothing to send (no files found)".into()));
    }
    Ok(out)
}

fn file_name(p: &Path) -> Result<String> {
    // canonicalize so "." and "dir/.." get a real name.
    let abs = std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    abs.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| Error::Other(format!("{} has no name", p.display())))
}

#[derive(Debug, Clone)]
pub enum SendEvent {
    /// Request sent; the other person is deciding.
    WaitingForAccept,
    Accepted,
    FileStarted {
        file_id: String,
        size: u64,
    },
    FileProgress {
        file_id: String,
        sent: u64,
    },
    FileDone {
        file_id: String,
        saved_as: String,
    },
    FileFailed {
        file_id: String,
        error: String,
    },
    /// Retrying a file after a network error.
    FileRetry {
        file_id: String,
        attempt: usize,
    },
}

#[derive(Debug, Clone, Default)]
pub struct SendSummary {
    pub sent: usize,
    pub failed: usize,
    pub bytes: u64,
}

/// Send `files` to the device at `addr`.
///
/// Returns once every file is saved or has failed. Errors only for problems
/// that stop the whole transfer (declined, busy, unreachable, cancelled).
pub async fn send_files(
    addr: SocketAddr,
    me: &DeviceInfo,
    files: Vec<SourceFile>,
    events: mpsc::UnboundedSender<SendEvent>,
    cancel: CancellationToken,
) -> Result<SendSummary> {
    let client = http_client();
    let req = PrepareRequest {
        sender: me.clone(),
        files: files
            .iter()
            .map(|f| FileMeta {
                id: f.id.clone(),
                path: f.rel.clone(),
                size: f.size,
            })
            .collect(),
    };

    let _ = events.send(SendEvent::WaitingForAccept);
    let prep = client
        .post(url(addr, "/prepare"))
        .timeout(Duration::from_secs(CONSENT_TIMEOUT_SECS + 15))
        .json(&req)
        .send();
    let resp = tokio::select! {
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        r = prep => r?,
    };
    let prep: PrepareResponse = check(resp).await?.json().await?;
    let _ = events.send(SendEvent::Accepted);

    let session = prep.session_id.clone();
    let tokens = Arc::new(prep.tokens);

    let results: Vec<(u64, bool)> = stream::iter(files)
        .map(|f| {
            let client = client.clone();
            let events = events.clone();
            let session = session.clone();
            let tokens = tokens.clone();
            let cancel = cancel.clone();
            async move {
                let Some(token) = tokens.get(&f.id).cloned() else {
                    let _ = events.send(SendEvent::FileFailed {
                        file_id: f.id.clone(),
                        error: "receiver skipped this file".into(),
                    });
                    return (0, false);
                };
                let _ = events.send(SendEvent::FileStarted {
                    file_id: f.id.clone(),
                    size: f.size,
                });
                let mut attempt = 0;
                loop {
                    if cancel.is_cancelled() {
                        return (0, false);
                    }
                    let r = send_one(&client, addr, &session, &token, &f, &events, &cancel).await;
                    match r {
                        Ok(saved_as) => {
                            let _ = events.send(SendEvent::FileDone {
                                file_id: f.id.clone(),
                                saved_as,
                            });
                            return (f.size, true);
                        }
                        Err(e) if attempt < UPLOAD_RETRIES && retryable(&e) => {
                            attempt += 1;
                            let _ = events.send(SendEvent::FileRetry {
                                file_id: f.id.clone(),
                                attempt,
                            });
                            tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                        }
                        Err(e) => {
                            let _ = events.send(SendEvent::FileFailed {
                                file_id: f.id.clone(),
                                error: e.to_string(),
                            });
                            return (0, false);
                        }
                    }
                }
            }
        })
        .buffer_unordered(PARALLEL_UPLOADS)
        .collect()
        .await;

    if cancel.is_cancelled() {
        let _ = client
            .post(url(addr, &format!("/cancel?session={session}")))
            .timeout(Duration::from_secs(4))
            .send()
            .await;
        return Err(Error::Cancelled);
    }

    let _ = client
        .post(url(addr, &format!("/complete?session={session}")))
        .timeout(Duration::from_secs(4))
        .send()
        .await;

    let mut summary = SendSummary::default();
    for (bytes, ok) in results {
        if ok {
            summary.sent += 1;
            summary.bytes += bytes;
        } else {
            summary.failed += 1;
        }
    }
    Ok(summary)
}

fn retryable(e: &Error) -> bool {
    match e {
        Error::Http(h) => h.is_connect() || h.is_timeout() || h.is_request() || h.is_body(),
        Error::ChecksumMismatch(_) => true,
        Error::Io(_) => false,
        Error::Protocol(m) => m.starts_with("400"),
        _ => false,
    }
}

/// Upload one file and finalize it. Returns the name it was saved as.
async fn send_one(
    client: &reqwest::Client,
    addr: SocketAddr,
    session: &str,
    token: &str,
    f: &SourceFile,
    events: &mpsc::UnboundedSender<SendEvent>,
    cancel: &CancellationToken,
) -> Result<String> {
    let q = FileQuery {
        session: session.to_string(),
        file: f.id.clone(),
        token: token.to_string(),
    };
    let query = serde_urlencode(&q);

    let file = tokio::fs::File::open(&f.abs).await?;
    // Never send more than we announced, even if the file grew.
    let reader = file.take(f.size);
    let hasher = Arc::new(Mutex::new(Sha256::new()));
    let h2 = hasher.clone();
    let events2 = events.clone();
    let file_id = f.id.clone();
    let mut sent: u64 = 0;
    let mut last = std::time::Instant::now();
    let body_stream =
        ReaderStream::with_capacity(reader, READ_CHUNK).map_ok(move |chunk: Bytes| {
            h2.lock().unwrap().update(&chunk);
            sent += chunk.len() as u64;
            if last.elapsed() >= Duration::from_millis(100) {
                last = std::time::Instant::now();
                let _ = events2.send(SendEvent::FileProgress {
                    file_id: file_id.clone(),
                    sent,
                });
            }
            chunk
        });

    let put = client
        .put(url(addr, &format!("/upload?{query}")))
        .header(reqwest::header::CONTENT_LENGTH, f.size)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .body(reqwest::Body::wrap_stream(body_stream))
        .send();
    let resp = tokio::select! {
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        r = put => r?,
    };
    let up: UploadResponse = check(resp).await?.json().await?;
    let _ = events.send(SendEvent::FileProgress {
        file_id: f.id.clone(),
        sent: up.bytes,
    });

    let ours = hex::encode(std::mem::take(&mut *hasher.lock().unwrap()).finalize());
    if up.bytes != f.size {
        return Err(Error::Protocol(format!(
            "receiver got {} of {} bytes",
            up.bytes, f.size
        )));
    }
    // Finalize with OUR hash; the receiver compares against its own.
    let resp = client
        .post(url(addr, &format!("/finalize?{query}")))
        .timeout(Duration::from_secs(30))
        .json(&FinalizeRequest { sha256: ours })
        .send()
        .await?;
    let fin: FinalizeResponse = check(resp).await?.json().await?;
    Ok(fin.saved_as)
}

/// Minimal query-string encoding for our own (hex/alnum) values.
fn serde_urlencode(q: &FileQuery) -> String {
    fn enc(s: &str) -> String {
        s.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                    (b as char).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect()
    }
    format!(
        "session={}&file={}&token={}",
        enc(&q.session),
        enc(&q.file),
        enc(&q.token)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_files_and_folders() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().join("Trip");
        std::fs::create_dir_all(root.join("day1")).unwrap();
        std::fs::write(root.join("day1/a.jpg"), b"aaa").unwrap();
        std::fs::write(root.join("notes.txt"), b"n").unwrap();
        let single = d.path().join("single.bin");
        std::fs::write(&single, b"12345").unwrap();

        let files = collect_files(&[root, single]).unwrap();
        let rels: Vec<&str> = files.iter().map(|f| f.rel.as_str()).collect();
        assert_eq!(
            rels,
            vec!["Trip/day1/a.jpg", "Trip/notes.txt", "single.bin"]
        );
        assert_eq!(files[2].size, 5);
        let ids: Vec<&str> = files.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, vec!["f0", "f1", "f2"]);
    }

    #[test]
    fn empty_selection_errors() {
        let d = tempfile::tempdir().unwrap();
        assert!(collect_files(&[d.path().to_path_buf()]).is_err());
        assert!(collect_files(&[d.path().join("missing")]).is_err());
    }

    #[test]
    fn query_encoding() {
        let q = FileQuery {
            session: "ab12".into(),
            file: "f-1".into(),
            token: "x y".into(),
        };
        assert_eq!(serde_urlencode(&q), "session=ab12&file=f-1&token=x%20y");
    }
}
