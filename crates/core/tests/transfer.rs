//! End-to-end tests: a real receive server and a real sender on loopback.

use magicdrop_core::client::{self, collect_files, SendEvent};
use magicdrop_core::protocol::{
    FileMeta, FileQuery, FinalizeRequest, PrepareRequest, PrepareResponse, API_PREFIX,
};
use magicdrop_core::server::{self, ReceiveEvent, ServerConfig};
use magicdrop_core::{Error, Identity};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Receiver {
    _home: tempfile::TempDir,
    downloads: tempfile::TempDir,
    server: server::RunningServer,
    events: mpsc::Receiver<ReceiveEvent>,
}

impl Receiver {
    async fn start() -> Self {
        let home = tempfile::tempdir().unwrap();
        let downloads = tempfile::tempdir().unwrap();
        let id = Identity::load_or_create(home.path()).unwrap();
        let mut cfg = ServerConfig::new(downloads.path());
        cfg.port = 0;
        let (server, events) = server::start(&id, cfg, None).await.unwrap();
        Self {
            _home: home,
            downloads,
            server,
            events,
        }
    }

    fn addr(&self) -> SocketAddr {
        SocketAddr::new(Ipv4Addr::LOCALHOST.into(), self.server.port)
    }

    /// Answer every request with `accept`, and collect all events.
    fn auto(
        mut self,
        accept: bool,
    ) -> (
        PathBuf,
        SocketAddr,
        tokio::task::JoinHandle<Vec<String>>,
        server::RunningServer,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        let dir = self.downloads.path().to_path_buf();
        let addr = self.addr();
        let task = tokio::spawn(async move {
            let mut log = Vec::new();
            while let Some(ev) = self.events.recv().await {
                match ev {
                    ReceiveEvent::Request(r) => {
                        log.push(format!("request:{}", r.files.len()));
                        if accept {
                            r.accept()
                        } else {
                            r.decline()
                        }
                    }
                    ReceiveEvent::FileSaved { .. } => log.push("saved".into()),
                    ReceiveEvent::FileFailed { error, .. } => log.push(format!("failed:{error}")),
                    ReceiveEvent::SessionFinished { saved, failed, .. } => {
                        log.push(format!("finished:{saved}:{failed}"))
                    }
                    ReceiveEvent::SessionCancelled { reason, .. } => {
                        log.push(format!("cancelled:{reason}"))
                    }
                    _ => {}
                }
            }
            log
        });
        (dir, addr, task, self.server, self._home, self.downloads)
    }
}

fn sender_identity() -> (tempfile::TempDir, magicdrop_core::DeviceInfo) {
    let home = tempfile::tempdir().unwrap();
    let id = Identity::load_or_create(home.path()).unwrap();
    let info = id.device_info(1);
    (home, info)
}

fn pseudo_random_bytes(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn read(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

#[tokio::test]
async fn sends_files_and_folders_intact() {
    let rx = Receiver::start().await;
    let (dir, addr, log, _srv, _h, _d) = rx.auto(true);

    let src = tempfile::tempdir().unwrap();
    let big = pseudo_random_bytes(12 * 1024 * 1024 + 7, 42);
    std::fs::write(src.path().join("big.bin"), &big).unwrap();
    std::fs::write(src.path().join("empty.txt"), b"").unwrap();
    std::fs::write(src.path().join("résumé 2026.pdf"), b"pdf!").unwrap();
    let album = src.path().join("Album");
    std::fs::create_dir_all(album.join("nested/deeper")).unwrap();
    for i in 0..25 {
        std::fs::write(
            album.join(format!("img{i:02}.jpg")),
            pseudo_random_bytes(3000 + i, i as u64),
        )
        .unwrap();
    }
    std::fs::write(album.join("nested/deeper/x.txt"), b"deep").unwrap();

    let files = collect_files(&[
        src.path().join("big.bin"),
        src.path().join("empty.txt"),
        src.path().join("résumé 2026.pdf"),
        album.clone(),
    ])
    .unwrap();
    let n = files.len();
    let (_sh, me) = sender_identity();
    let (etx, mut erx) = mpsc::unbounded_channel();
    let summary = client::send_files(addr, &me, files, etx, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(summary.sent, n);
    assert_eq!(summary.failed, 0);

    let mut saw_accept = false;
    while let Ok(ev) = erx.try_recv() {
        if matches!(ev, SendEvent::Accepted) {
            saw_accept = true;
        }
    }
    assert!(saw_accept);

    assert_eq!(read(&dir.join("big.bin")), big);
    assert_eq!(read(&dir.join("empty.txt")), b"");
    assert_eq!(read(&dir.join("résumé 2026.pdf")), b"pdf!");
    assert_eq!(read(&dir.join("Album/nested/deeper/x.txt")), b"deep");
    for i in 0..25 {
        assert_eq!(
            read(&dir.join(format!("Album/img{i:02}.jpg"))),
            pseudo_random_bytes(3000 + i, i as u64)
        );
    }
    // No temp files left behind.
    let leftovers: Vec<_> = walk(&dir)
        .into_iter()
        .filter(|p| p.to_string_lossy().ends_with(".mdpart"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    drop(_srv);
    let log = log.await.unwrap();
    assert_eq!(log.first().unwrap(), &format!("request:{n}"));
    assert!(log.contains(&format!("finished:{n}:0")), "{log:?}");
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[tokio::test]
async fn existing_files_are_not_overwritten() {
    let rx = Receiver::start().await;
    let (dir, addr, _log, _srv, _h, _d) = rx.auto(true);
    std::fs::write(dir.join("report.pdf"), b"original").unwrap();

    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("report.pdf"), b"new one").unwrap();
    let (_sh, me) = sender_identity();
    for _ in 0..2 {
        let files = collect_files(&[src.path().join("report.pdf")]).unwrap();
        let (etx, _erx) = mpsc::unbounded_channel();
        client::send_files(addr, &me, files, etx, CancellationToken::new())
            .await
            .unwrap();
    }
    assert_eq!(read(&dir.join("report.pdf")), b"original");
    assert_eq!(read(&dir.join("report (1).pdf")), b"new one");
    assert_eq!(read(&dir.join("report (2).pdf")), b"new one");
}

#[tokio::test]
async fn declined_transfer_writes_nothing() {
    let rx = Receiver::start().await;
    let (dir, addr, _log, _srv, _h, _d) = rx.auto(false);
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("a.txt"), b"a").unwrap();
    let files = collect_files(&[src.path().join("a.txt")]).unwrap();
    let (_sh, me) = sender_identity();
    let (etx, _erx) = mpsc::unbounded_channel();
    let err = client::send_files(addr, &me, files, etx, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Declined), "{err:?}");
    assert!(walk(&dir).is_empty());
}

#[tokio::test]
async fn second_sender_gets_busy_while_first_waits() {
    let mut rx = Receiver::start().await;
    let addr = rx.addr();
    let (_sh, me) = sender_identity();
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("a.txt"), b"a").unwrap();

    let files = collect_files(&[src.path().join("a.txt")]).unwrap();
    let me1 = me.clone();
    let first = tokio::spawn(async move {
        let (etx, _erx) = mpsc::unbounded_channel();
        client::send_files(addr, &me1, files, etx, CancellationToken::new()).await
    });
    // Wait until the first request is pending.
    let req = match rx.events.recv().await.unwrap() {
        ReceiveEvent::Request(r) => r,
        other => panic!("unexpected {other:?}"),
    };
    let files = collect_files(&[src.path().join("a.txt")]).unwrap();
    let (etx, _erx) = mpsc::unbounded_channel();
    let err = client::send_files(addr, &me, files, etx, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Busy), "{err:?}");
    req.decline();
    assert!(matches!(first.await.unwrap(), Err(Error::Declined)));
}

/// Talk to the server directly to exercise hostile inputs.
async fn raw_prepare(addr: SocketAddr, files: Vec<FileMeta>) -> reqwest::Response {
    let (_sh, me) = sender_identity();
    client::http_client()
        .post(format!("http://{addr}{API_PREFIX}/prepare"))
        .json(&PrepareRequest { sender: me, files })
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn hostile_paths_are_rejected_before_asking_the_user() {
    let rx = Receiver::start().await;
    let (_dir, addr, log, srv, _h, _d) = rx.auto(true);
    for bad in [
        "../evil.txt",
        "/etc/passwd",
        "C:\\x",
        "a/../../b",
        "x:stream",
    ] {
        let resp = raw_prepare(
            addr,
            vec![FileMeta {
                id: "f0".into(),
                path: bad.into(),
                size: 1,
            }],
        )
        .await;
        assert_eq!(resp.status(), 400, "{bad}");
    }
    let resp = raw_prepare(
        addr,
        vec![
            FileMeta {
                id: "dup".into(),
                path: "a".into(),
                size: 1,
            },
            FileMeta {
                id: "dup".into(),
                path: "b".into(),
                size: 1,
            },
        ],
    )
    .await;
    assert_eq!(resp.status(), 400);
    drop(srv);
    assert!(
        log.await.unwrap().is_empty(),
        "user should never have been asked"
    );
}

#[tokio::test]
async fn bad_checksum_and_oversize_are_rejected() {
    let rx = Receiver::start().await;
    let (dir, addr, _log, _srv, _h, _d) = rx.auto(true);
    let resp = raw_prepare(
        addr,
        vec![
            FileMeta {
                id: "a".into(),
                path: "a.txt".into(),
                size: 5,
            },
            FileMeta {
                id: "b".into(),
                path: "b.txt".into(),
                size: 2,
            },
        ],
    )
    .await;
    assert_eq!(resp.status(), 200);
    let prep: PrepareResponse = resp.json().await.unwrap();
    let http = client::http_client();
    let q = |id: &str| {
        let fq = FileQuery {
            session: prep.session_id.clone(),
            file: id.into(),
            token: prep.tokens[id].clone(),
        };
        format!("session={}&file={}&token={}", fq.session, fq.file, fq.token)
    };

    // Wrong token.
    let r = http
        .put(format!(
            "http://{addr}{API_PREFIX}/upload?session={}&file=a&token=nope",
            prep.session_id
        ))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // Correct upload, wrong checksum at finalize.
    let r = http
        .put(format!("http://{addr}{API_PREFIX}/upload?{}", q("a")))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = http
        .post(format!("http://{addr}{API_PREFIX}/finalize?{}", q("a")))
        .json(&FinalizeRequest {
            sha256: "00".repeat(32),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);
    assert!(!dir.join("a.txt").exists());

    // More bytes than announced.
    let r = http
        .put(format!("http://{addr}{API_PREFIX}/upload?{}", q("b")))
        .body("way too long")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    assert!(!dir.join("b.txt").exists());

    // Nothing half-written left behind.
    assert!(walk(&dir).is_empty(), "{:?}", walk(&dir));
}

#[tokio::test]
async fn sender_cancel_cleans_up() {
    let rx = Receiver::start().await;
    let (dir, addr, log, srv, _h, _d) = rx.auto(true);
    let src = tempfile::tempdir().unwrap();
    std::fs::write(
        src.path().join("big.bin"),
        pseudo_random_bytes(64 * 1024 * 1024, 7),
    )
    .unwrap();
    let files = collect_files(&[src.path().join("big.bin")]).unwrap();
    let (_sh, me) = sender_identity();
    let (etx, mut erx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        while let Some(ev) = erx.recv().await {
            if let SendEvent::FileProgress { sent, .. } = ev {
                if sent > 0 {
                    c2.cancel();
                }
            }
        }
    });
    let r = client::send_files(addr, &me, files, etx, cancel).await;
    assert!(matches!(r, Err(Error::Cancelled)), "{r:?}");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!dir.join("big.bin").exists());
    let leftovers: Vec<_> = walk(&dir);
    assert!(leftovers.is_empty(), "{leftovers:?}");
    drop(srv);
    let log = log.await.unwrap();
    assert!(log.iter().any(|l| l.starts_with("cancelled")), "{log:?}");
}

#[tokio::test]
async fn info_and_register_roundtrip() {
    let rx = Receiver::start().await;
    let addr = rx.addr();
    let info = client::info(addr).await.unwrap();
    assert_eq!(info.port, addr.port());
    let (_sh, me) = sender_identity();
    let theirs = client::register(addr, &me).await.unwrap();
    assert_eq!(theirs.id, info.id);
}
