//! The receive side: an HTTP server that accepts incoming transfers.
//!
//! The server never writes a byte to disk until the local user accepts. The
//! front-end gets a [`ReceiveEvent::Request`] and answers it; everything after
//! that (progress, saved files, completion) arrives as further events.

use crate::error::Result;
use crate::identity::Identity;
use crate::paths::{safe_join, sanitize_relative_path, unique_path};
use crate::peers::{PeerRegistry, Source};
use crate::protocol::*;
use crate::util::{ct_eq, random_token};
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

/// Max files in one transfer.
const MAX_FILES: usize = 100_000;
/// Drop a session after this long with no activity.
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const WRITE_BUFFER: usize = 1 << 20;
const PROGRESS_EVERY: Duration = Duration::from_millis(100);
const PART_SUFFIX: &str = ".mdpart";

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Where accepted files are saved.
    pub download_dir: PathBuf,
    /// Port to listen on. If it's taken, a random port is used instead.
    pub port: u16,
    /// Also listen on IPv6.
    pub ipv6: bool,
    /// When false the server only answers discovery (`/info`, `/register`)
    /// and refuses transfers. Used by `send` and `devices`.
    pub accept_transfers: bool,
}

impl ServerConfig {
    pub fn new(download_dir: impl Into<PathBuf>) -> Self {
        Self {
            download_dir: download_dir.into(),
            port: DEFAULT_PORT,
            ipv6: true,
            accept_transfers: true,
        }
    }
}

/// Default download folder: `~/Downloads/MagicDrop`.
pub fn default_download_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join("Downloads")))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("MagicDrop")
}

/// Things the front-end needs to know about.
#[derive(Debug)]
pub enum ReceiveEvent {
    /// Someone wants to send files. Call `accept()` or `decline()`.
    Request(IncomingRequest),
    FileStarted {
        session_id: String,
        file_id: String,
        path: String,
        size: u64,
    },
    FileProgress {
        session_id: String,
        file_id: String,
        received: u64,
        size: u64,
    },
    FileSaved {
        session_id: String,
        file_id: String,
        saved_to: PathBuf,
    },
    FileFailed {
        session_id: String,
        file_id: String,
        error: String,
    },
    SessionFinished {
        session_id: String,
        saved: usize,
        failed: usize,
    },
    SessionCancelled {
        session_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Accept,
    Decline,
}

/// An incoming transfer waiting for the user's decision.
#[derive(Debug)]
pub struct IncomingRequest {
    pub session_id: String,
    pub sender: DeviceInfo,
    pub sender_addr: IpAddr,
    pub files: Vec<FileMeta>,
    pub total_bytes: u64,
    respond: oneshot::Sender<Decision>,
}

impl IncomingRequest {
    pub fn accept(self) {
        let _ = self.respond.send(Decision::Accept);
    }
    pub fn decline(self) {
        let _ = self.respond.send(Decision::Decline);
    }
    /// True if the sender gave up waiting (timed out or disconnected).
    pub fn is_expired(&self) -> bool {
        self.respond.is_closed()
    }
}

/// A running server. Dropping it stops the server.
pub struct RunningServer {
    pub port: u16,
    pub info: DeviceInfo,
    state: Arc<AppState>,
    shutdown: CancellationToken,
}

impl RunningServer {
    /// Cancel whatever transfer is in progress (receiver-side cancel).
    pub async fn cancel_active(&self) {
        let mut guard = self.state.session.lock().await;
        if let Some(s) = guard.take() {
            s.abort(&self.state, "cancelled by receiver").await;
        }
    }

    /// The device we're currently receiving from, if any.
    pub async fn active_sender(&self) -> Option<DeviceInfo> {
        self.state
            .session
            .lock()
            .await
            .as_ref()
            .map(|s| s.sender.clone())
    }

    pub fn download_dir(&self) -> &Path {
        &self.state.dir
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct AppState {
    me: DeviceInfo,
    dir: PathBuf,
    events: mpsc::Sender<ReceiveEvent>,
    session: Mutex<Option<Session>>,
    /// Set while a request is waiting for the user's decision.
    pending: AtomicBool,
    accept_transfers: bool,
    registry: Option<PeerRegistry>,
    /// Serialises "pick a unique name + rename" so two files can't grab the
    /// same name.
    finalize_lock: Mutex<()>,
}

struct Session {
    id: String,
    sender: DeviceInfo,
    sender_ip: IpAddr,
    files: HashMap<String, FileSlot>,
    last_activity: Instant,
    saved: usize,
    failed: usize,
    cancel: CancellationToken,
}

struct FileSlot {
    meta: FileMeta,
    rel: PathBuf,
    token: String,
    state: SlotState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SlotState {
    Pending,
    Uploading,
    Uploaded { part: PathBuf, sha256: String },
    Saved,
    Failed,
}

impl Session {
    async fn abort(self, state: &AppState, reason: &str) {
        self.cancel.cancel();
        for slot in self.files.values() {
            if let SlotState::Uploaded { part, .. } = &slot.state {
                let _ = tokio::fs::remove_file(part).await;
            }
        }
        // Parts still being written are removed by their upload handler when
        // it sees the cancellation.
        let _ = state
            .events
            .send(ReceiveEvent::SessionCancelled {
                session_id: self.id,
                reason: reason.to_string(),
            })
            .await;
    }

    fn uploading(&self) -> bool {
        self.files.values().any(|f| f.state == SlotState::Uploading)
    }
}

/// Start the receive server.
///
/// `registry`, if given, is fed devices that call our `/register` endpoint.
pub async fn start(
    identity: &Identity,
    cfg: ServerConfig,
    registry: Option<PeerRegistry>,
) -> Result<(RunningServer, mpsc::Receiver<ReceiveEvent>)> {
    if cfg.accept_transfers {
        tokio::fs::create_dir_all(&cfg.download_dir).await?;
    }

    let v4 = bind_v4(cfg.port)?;
    let port = v4.local_addr()?.port();
    let v6 = if cfg.ipv6 {
        match bind_v6(port) {
            Ok(l) => Some(l),
            Err(e) => {
                tracing::debug!("IPv6 listener unavailable: {e}");
                None
            }
        }
    } else {
        None
    };

    let (tx, rx) = mpsc::channel(1024);
    let me = identity.device_info(port);
    let state = Arc::new(AppState {
        me: me.clone(),
        dir: cfg.download_dir.clone(),
        events: tx,
        session: Mutex::new(None),
        pending: AtomicBool::new(false),
        accept_transfers: cfg.accept_transfers,
        registry,
        finalize_lock: Mutex::new(()),
    });

    let app = router(state.clone());
    let shutdown = CancellationToken::new();

    for listener in std::iter::once(v4).chain(v6) {
        let app = app.clone();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            let svc = app.into_make_service_with_connect_info::<SocketAddr>();
            if let Err(e) = axum::serve(listener, svc)
                .with_graceful_shutdown(async move { sd.cancelled().await })
                .await
            {
                tracing::error!("server error: {e}");
            }
        });
    }

    // Janitor: expire idle sessions so a vanished sender doesn't block us.
    let st = state.clone();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = sd.cancelled() => break,
                _ = tick.tick() => {}
            }
            let mut guard = st.session.lock().await;
            let stale = guard.as_ref().is_some_and(|s| {
                !s.uploading() && s.last_activity.elapsed() > SESSION_IDLE_TIMEOUT
            });
            if stale {
                if let Some(s) = guard.take() {
                    s.abort(&st, "sender stopped responding").await;
                }
            }
        }
    });

    tracing::info!("receive server listening on port {port}");
    Ok((
        RunningServer {
            port,
            info: me,
            state,
            shutdown,
        },
        rx,
    ))
}

fn bind_v4(port: u16) -> Result<tokio::net::TcpListener> {
    let try_bind = |p: u16| -> std::io::Result<std::net::TcpListener> {
        let s = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
        #[cfg(unix)]
        s.set_reuse_address(true)?;
        s.bind(&SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), p).into())?;
        s.listen(128)?;
        s.set_nonblocking(true)?;
        Ok(s.into())
    };
    let std = match try_bind(port) {
        Ok(l) => l,
        Err(e) if port != 0 => {
            tracing::info!("port {port} unavailable ({e}); using a random port");
            try_bind(0)?
        }
        Err(e) => return Err(e.into()),
    };
    Ok(tokio::net::TcpListener::from_std(std)?)
}

fn bind_v6(port: u16) -> std::io::Result<tokio::net::TcpListener> {
    let s = socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::STREAM, None)?;
    s.set_only_v6(true)?;
    #[cfg(unix)]
    s.set_reuse_address(true)?;
    s.bind(&SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port).into())?;
    s.listen(128)?;
    s.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(s.into())
}

fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(&format!("{API_PREFIX}/info"), get(info))
        .route(&format!("{API_PREFIX}/register"), post(register))
        .route(&format!("{API_PREFIX}/prepare"), post(prepare))
        .route(
            &format!("{API_PREFIX}/upload"),
            put(upload).layer(DefaultBodyLimit::disable()),
        )
        .route(&format!("{API_PREFIX}/finalize"), post(finalize))
        .route(&format!("{API_PREFIX}/complete"), post(complete))
        .route(&format!("{API_PREFIX}/cancel"), post(cancel))
        .layer(DefaultBodyLimit::max(16 << 20))
        .with_state(state)
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(ErrorBody { error: msg.into() })).into_response()
}

/// Treat IPv4-mapped IPv6 (::ffff:a.b.c.d) as the IPv4 address it is.
fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

async fn info(State(st): State<Arc<AppState>>) -> Json<DeviceInfo> {
    Json(st.me.clone())
}

async fn register(
    State(st): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Json(mut their): Json<DeviceInfo>,
) -> Response {
    if their.protocol != PROTOCOL_VERSION {
        return err(StatusCode::BAD_REQUEST, "unsupported protocol version");
    }
    their.alias = crate::identity::clean_alias(&their.alias);
    if let Some(reg) = &st.registry {
        reg.upsert(their, canonical_ip(remote.ip()), Source::Register);
    }
    Json(st.me.clone()).into_response()
}

/// Resets the "pending" flag however the prepare handler exits (including the
/// sender disconnecting while we wait for the user).
struct PendingGuard<'a>(&'a AtomicBool);
impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

async fn prepare(
    State(st): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Json(req): Json<PrepareRequest>,
) -> Response {
    let sender_ip = canonical_ip(remote.ip());
    if !st.accept_transfers {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "this device isn't receiving right now",
        );
    }

    // ---- validate ----
    if req.sender.protocol != PROTOCOL_VERSION {
        return err(StatusCode::BAD_REQUEST, "unsupported protocol version");
    }
    if req.files.is_empty() {
        return err(StatusCode::BAD_REQUEST, "no files");
    }
    if req.files.len() > MAX_FILES {
        return err(StatusCode::BAD_REQUEST, "too many files");
    }
    let mut ids = HashSet::new();
    let mut total: u64 = 0;
    let mut slots = HashMap::new();
    for f in &req.files {
        if !valid_file_id(&f.id) || !ids.insert(f.id.clone()) {
            return err(StatusCode::BAD_REQUEST, format!("bad file id {:?}", f.id));
        }
        let rel = match sanitize_relative_path(&f.path) {
            Ok(p) => p,
            Err(e) => return err(StatusCode::BAD_REQUEST, e.to_string()),
        };
        total = match total.checked_add(f.size) {
            Some(t) => t,
            None => return err(StatusCode::BAD_REQUEST, "size overflow"),
        };
        slots.insert(
            f.id.clone(),
            FileSlot {
                meta: f.clone(),
                rel,
                token: random_token(16),
                state: SlotState::Pending,
            },
        );
    }

    // ---- one transfer at a time ----
    {
        let guard = st.session.lock().await;
        if guard.is_some() {
            return err(StatusCode::CONFLICT, "busy with another transfer");
        }
    }
    if st
        .pending
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return err(
            StatusCode::CONFLICT,
            "another request is waiting for an answer",
        );
    }
    let _pending = PendingGuard(&st.pending);

    // ---- ask the user ----
    let session_id = random_token(12);
    let mut sender = req.sender.clone();
    sender.alias = crate::identity::clean_alias(&sender.alias);
    let (tx, rx) = oneshot::channel();
    let request = IncomingRequest {
        session_id: session_id.clone(),
        sender: sender.clone(),
        sender_addr: sender_ip,
        files: req.files.clone(),
        total_bytes: total,
        respond: tx,
    };
    if st
        .events
        .send(ReceiveEvent::Request(request))
        .await
        .is_err()
    {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "receiver is not accepting files",
        );
    }
    let decision = tokio::time::timeout(Duration::from_secs(CONSENT_TIMEOUT_SECS), rx).await;
    match decision {
        Ok(Ok(Decision::Accept)) => {}
        Ok(Ok(Decision::Decline)) | Ok(Err(_)) => {
            return err(StatusCode::FORBIDDEN, "declined");
        }
        Err(_) => return err(StatusCode::FORBIDDEN, "no answer (timed out)"),
    }

    // ---- open the session ----
    let tokens: HashMap<String, String> = slots
        .iter()
        .map(|(id, s)| (id.clone(), s.token.clone()))
        .collect();
    let mut guard = st.session.lock().await;
    if guard.is_some() {
        return err(StatusCode::CONFLICT, "busy with another transfer");
    }
    *guard = Some(Session {
        id: session_id.clone(),
        sender,
        sender_ip,
        files: slots,
        last_activity: Instant::now(),
        saved: 0,
        failed: 0,
        cancel: CancellationToken::new(),
    });
    drop(guard);

    Json(PrepareResponse { session_id, tokens }).into_response()
}

/// Check session/token/address; returns a 4xx response on failure.
#[allow(clippy::result_large_err)] // the error is the HTTP response itself
fn authorize<'a>(
    guard: &'a mut Option<Session>,
    q: &FileQuery,
    ip: IpAddr,
) -> std::result::Result<&'a mut Session, Response> {
    let Some(s) = guard.as_mut() else {
        return Err(err(StatusCode::NOT_FOUND, "no active session"));
    };
    if !ct_eq(&s.id, &q.session) {
        return Err(err(StatusCode::NOT_FOUND, "unknown session"));
    }
    if s.sender_ip != ip {
        return Err(err(StatusCode::FORBIDDEN, "wrong sender address"));
    }
    match s.files.get(&q.file) {
        Some(slot) if ct_eq(&slot.token, &q.token) => Ok(s),
        Some(_) => Err(err(StatusCode::FORBIDDEN, "bad token")),
        None => Err(err(StatusCode::NOT_FOUND, "unknown file")),
    }
}

async fn upload(
    State(st): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Query(q): Query<FileQuery>,
    body: Body,
) -> Response {
    let ip = canonical_ip(remote.ip());

    // ---- claim the slot ----
    let (part, size, rel, cancel, session_id) = {
        let mut guard = st.session.lock().await;
        let s = match authorize(&mut guard, &q, ip) {
            Ok(s) => s,
            Err(r) => return r,
        };
        s.last_activity = Instant::now();
        let cancel = s.cancel.clone();
        let session_id = s.id.clone();
        let slot = s.files.get_mut(&q.file).expect("authorized");
        match &slot.state {
            SlotState::Pending | SlotState::Failed => {}
            SlotState::Uploaded { part, .. } => {
                let _ = std::fs::remove_file(part);
            }
            SlotState::Uploading => {
                return err(StatusCode::CONFLICT, "file is already being uploaded")
            }
            SlotState::Saved => return err(StatusCode::CONFLICT, "file already saved"),
        }
        let target = match safe_join(&st.dir, &slot.rel) {
            Ok(t) => t,
            Err(e) => return err(StatusCode::BAD_REQUEST, e.to_string()),
        };
        let parent = target.parent().unwrap_or(&st.dir).to_path_buf();
        let file_name = target
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into());
        let part = parent.join(format!(".{file_name}.{}{PART_SUFFIX}", slot.meta.id));
        slot.state = SlotState::Uploading;
        (
            part,
            slot.meta.size,
            slot.rel.to_string_lossy().replace('\\', "/"),
            cancel,
            session_id,
        )
    };

    let _ = st
        .events
        .send(ReceiveEvent::FileStarted {
            session_id: session_id.clone(),
            file_id: q.file.clone(),
            path: rel.clone(),
            size,
        })
        .await;

    let result = receive_body(&st, &session_id, &q.file, &part, size, body, &cancel).await;

    // ---- record the outcome ----
    let mut guard = st.session.lock().await;
    let still_ours = guard.as_ref().is_some_and(|s| s.id == session_id);
    match result {
        Ok((bytes, sha256)) if still_ours => {
            let s = guard.as_mut().expect("checked");
            s.last_activity = Instant::now();
            if let Some(slot) = s.files.get_mut(&q.file) {
                slot.state = SlotState::Uploaded {
                    part,
                    sha256: sha256.clone(),
                };
            }
            Json(UploadResponse {
                file_id: q.file,
                bytes,
                sha256,
            })
            .into_response()
        }
        Ok(_) => {
            let _ = tokio::fs::remove_file(&part).await;
            err(StatusCode::GONE, "transfer was cancelled")
        }
        Err((status, msg)) => {
            let _ = tokio::fs::remove_file(&part).await;
            if still_ours {
                let s = guard.as_mut().expect("checked");
                if let Some(slot) = s.files.get_mut(&q.file) {
                    slot.state = SlotState::Failed;
                }
            }
            drop(guard);
            let _ = st
                .events
                .send(ReceiveEvent::FileFailed {
                    session_id,
                    file_id: q.file,
                    error: msg.clone(),
                })
                .await;
            err(status, msg)
        }
    }
}

/// Stream the request body into `part`, hashing as we go.
async fn receive_body(
    st: &AppState,
    session_id: &str,
    file_id: &str,
    part: &Path,
    size: u64,
    body: Body,
    cancel: &CancellationToken,
) -> std::result::Result<(u64, String), (StatusCode, String)> {
    let io_err = |e: std::io::Error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("disk error: {e}"),
        )
    };

    if let Some(parent) = part.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(io_err)?;
    }
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(part)
        .await
        .map_err(io_err)?;
    let mut out = tokio::io::BufWriter::with_capacity(WRITE_BUFFER, file);
    let mut hasher = Sha256::new();
    let mut received: u64 = 0;
    let mut stream = body.into_data_stream();
    let mut last_progress = Instant::now();

    loop {
        let chunk = tokio::select! {
            _ = cancel.cancelled() => {
                return Err((StatusCode::GONE, "transfer was cancelled".into()));
            }
            c = stream.next() => c,
        };
        let Some(chunk) = chunk else { break };
        let chunk = chunk.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("connection lost while receiving: {e}"),
            )
        })?;
        received += chunk.len() as u64;
        if received > size {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "sender sent more data than announced".into(),
            ));
        }
        hasher.update(&chunk);
        out.write_all(&chunk).await.map_err(io_err)?;

        if last_progress.elapsed() >= PROGRESS_EVERY {
            last_progress = Instant::now();
            let _ = st.events.try_send(ReceiveEvent::FileProgress {
                session_id: session_id.to_string(),
                file_id: file_id.to_string(),
                received,
                size,
            });
            if let Some(s) = st.session.lock().await.as_mut() {
                s.last_activity = Instant::now();
            }
        }
    }
    if received != size {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("incomplete upload: got {received} of {size} bytes"),
        ));
    }
    out.flush().await.map_err(io_err)?;
    let file = out.into_inner();
    file.sync_all().await.map_err(io_err)?;
    drop(file);
    let _ = st.events.try_send(ReceiveEvent::FileProgress {
        session_id: session_id.to_string(),
        file_id: file_id.to_string(),
        received,
        size,
    });
    Ok((received, hex::encode(hasher.finalize())))
}

async fn finalize(
    State(st): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Query(q): Query<FileQuery>,
    Json(body): Json<FinalizeRequest>,
) -> Response {
    let ip = canonical_ip(remote.ip());
    let mut guard = st.session.lock().await;
    let s = match authorize(&mut guard, &q, ip) {
        Ok(s) => s,
        Err(r) => return r,
    };
    s.last_activity = Instant::now();
    let session_id = s.id.clone();
    let slot = s.files.get_mut(&q.file).expect("authorized");
    let (part, sha) = match &slot.state {
        SlotState::Uploaded { part, sha256 } => (part.clone(), sha256.clone()),
        SlotState::Saved => return err(StatusCode::CONFLICT, "already saved"),
        _ => return err(StatusCode::CONFLICT, "file has not been uploaded"),
    };

    if !ct_eq(&sha, &body.sha256.to_ascii_lowercase()) {
        let _ = tokio::fs::remove_file(&part).await;
        slot.state = SlotState::Failed;
        drop(guard);
        let _ = st
            .events
            .send(ReceiveEvent::FileFailed {
                session_id,
                file_id: q.file,
                error: "checksum mismatch".into(),
            })
            .await;
        return err(StatusCode::UNPROCESSABLE_ENTITY, "checksum mismatch");
    }

    let desired = match safe_join(&st.dir, &slot.rel) {
        Ok(p) => p,
        Err(e) => return err(StatusCode::BAD_REQUEST, e.to_string()),
    };
    let saved_to = {
        let _lock = st.finalize_lock.lock().await;
        let target = unique_path(&desired);
        if let Err(e) = tokio::fs::rename(&part, &target).await {
            let _ = tokio::fs::remove_file(&part).await;
            slot.state = SlotState::Failed;
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not save file: {e}"),
            );
        }
        target
    };
    slot.state = SlotState::Saved;
    s.saved += 1;
    let saved_as = saved_to
        .strip_prefix(&st.dir)
        .unwrap_or(&saved_to)
        .to_string_lossy()
        .replace('\\', "/");

    let all_saved = s.files.values().all(|f| f.state == SlotState::Saved);
    let finished = if all_saved {
        guard.take().map(|s| (s.id, s.saved, s.failed))
    } else {
        None
    };
    drop(guard);

    let _ = st
        .events
        .send(ReceiveEvent::FileSaved {
            session_id,
            file_id: q.file.clone(),
            saved_to,
        })
        .await;
    if let Some((session_id, saved, failed)) = finished {
        let _ = st
            .events
            .send(ReceiveEvent::SessionFinished {
                session_id,
                saved,
                failed,
            })
            .await;
    }

    Json(FinalizeResponse {
        file_id: q.file,
        saved_as,
    })
    .into_response()
}

/// Sender is done (possibly with some files failed).
async fn complete(
    State(st): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Query(q): Query<SessionQuery>,
) -> Response {
    let ip = canonical_ip(remote.ip());
    let mut guard = st.session.lock().await;
    match guard.as_ref() {
        Some(s) if ct_eq(&s.id, &q.session) && s.sender_ip == ip => {}
        // Already finished (all files saved) — that's fine.
        _ => return StatusCode::NO_CONTENT.into_response(),
    }
    let s = guard.take().expect("checked");
    drop(guard);
    let failed = s
        .files
        .values()
        .filter(|f| f.state != SlotState::Saved)
        .count();
    for f in s.files.values() {
        if let SlotState::Uploaded { part, .. } = &f.state {
            let _ = tokio::fs::remove_file(part).await;
        }
    }
    let _ = st
        .events
        .send(ReceiveEvent::SessionFinished {
            session_id: s.id,
            saved: s.saved,
            failed,
        })
        .await;
    StatusCode::NO_CONTENT.into_response()
}

async fn cancel(
    State(st): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Query(q): Query<SessionQuery>,
) -> Response {
    let ip = canonical_ip(remote.ip());
    let mut guard = st.session.lock().await;
    match guard.as_ref() {
        Some(s) if ct_eq(&s.id, &q.session) && s.sender_ip == ip => {
            let s = guard.take().expect("checked");
            drop(guard);
            s.abort(&st, "cancelled by sender").await;
            StatusCode::NO_CONTENT.into_response()
        }
        _ => err(StatusCode::NOT_FOUND, "unknown session"),
    }
}
