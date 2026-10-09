//! Wire protocol: constants and the JSON messages exchanged between devices.
//!
//! See `docs/PROTOCOL.md` for the full description. In short:
//!
//! 1. Discovery: devices advertise `_magicdrop._tcp` over mDNS and also send a
//!    JSON [`Announcement`] to a UDP multicast group. Either is enough.
//! 2. `POST /prepare` with file metadata. The receiver asks its user and either
//!    returns per-file upload tokens or 403.
//! 3. `PUT /upload` streams each file's raw bytes. The receiver writes to a
//!    temporary `.mdpart` file and returns the SHA-256 it computed.
//! 4. `POST /finalize` with the SHA-256 the sender computed. If they match the
//!    receiver moves the file into place.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::Ipv4Addr;

/// Protocol version spoken by this build. Bumped on breaking changes.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default TCP port for the HTTP API. If taken, we fall back to a random port
/// and advertise whichever one we actually got.
pub const DEFAULT_PORT: u16 = 47470;

/// UDP port for multicast announcements.
pub const MULTICAST_PORT: u16 = 47471;

/// Multicast group for announcements. It's in 224.0.0.0/24 (link-local, never
/// routed, TTL 1), which is the range home routers and phones are least likely
/// to filter.
pub const MULTICAST_GROUP_V4: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 199);

/// mDNS / DNS-SD service type.
pub const MDNS_SERVICE_TYPE: &str = "_magicdrop._tcp.local.";

/// Prefix for every HTTP endpoint.
pub const API_PREFIX: &str = "/api/magicdrop/v1";

/// How long the receiver's user has to accept a request before it auto-declines.
pub const CONSENT_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DeviceType {
    Desktop,
    Laptop,
    Phone,
    Tablet,
    Server,
    #[default]
    #[serde(other)]
    Other,
}

/// Who a device is. Sent in announcements, `/info`, and `/prepare`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// Stable device id: SHA-256 fingerprint of the device certificate (hex).
    pub id: String,
    /// Human-friendly name shown to other people ("Time's Laptop").
    pub alias: String,
    #[serde(default)]
    pub device_type: DeviceType,
    /// "windows", "macos", "linux", ...
    #[serde(default)]
    pub os: String,
    /// App version string.
    #[serde(default)]
    pub version: String,
    pub protocol: u32,
    /// TCP port of the HTTP API.
    pub port: u16,
}

impl DeviceInfo {
    /// Short form of the id, handy for display and disambiguation.
    pub fn short_id(&self) -> &str {
        let end = self.id.len().min(8);
        &self.id[..end]
    }
}

/// UDP multicast payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Announcement {
    /// Magic marker so we can ignore unrelated traffic on the group.
    pub magicdrop: u32,
    pub kind: AnnouncementKind,
    pub info: DeviceInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnnouncementKind {
    /// "I'm here, who else is?" Others respond.
    Announce,
    /// Response to an announce. Not answered further (prevents storms).
    Reply,
}

/// Metadata for one file in a transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    /// Sender-chosen id, unique within the request. `[A-Za-z0-9_-]{1,64}`.
    pub id: String,
    /// Relative path using `/` as separator, e.g. `photos/beach.jpg`.
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareRequest {
    pub sender: DeviceInfo,
    pub files: Vec<FileMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareResponse {
    pub session_id: String,
    /// fileId -> upload token
    pub tokens: HashMap<String, String>,
}

/// Returned by `PUT /upload` once the body is fully received.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadResponse {
    pub file_id: String,
    pub bytes: u64,
    /// Hex SHA-256 the receiver computed over what it got.
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeRequest {
    /// Hex SHA-256 the sender computed over what it sent.
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeResponse {
    pub file_id: String,
    /// Final file name on the receiver's disk (relative to its download folder).
    pub saved_as: String,
}

/// Query parameters shared by `/upload` and `/finalize`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileQuery {
    pub session: String,
    pub file: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionQuery {
    pub session: String,
}

/// Error body for any non-2xx response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

/// Is this a valid file id?
pub fn valid_file_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_type_unknown_value_maps_to_other() {
        let t: DeviceType = serde_json::from_str("\"toaster\"").unwrap();
        assert_eq!(t, DeviceType::Other);
    }

    #[test]
    fn file_id_validation() {
        assert!(valid_file_id("abc-123_X"));
        assert!(!valid_file_id(""));
        assert!(!valid_file_id("../etc"));
        assert!(!valid_file_id("a b"));
        assert!(!valid_file_id(&"a".repeat(65)));
    }

    #[test]
    fn announcement_roundtrip() {
        let a = Announcement {
            magicdrop: PROTOCOL_VERSION,
            kind: AnnouncementKind::Announce,
            info: DeviceInfo {
                id: "ab".repeat(32),
                alias: "Test".into(),
                device_type: DeviceType::Laptop,
                os: "linux".into(),
                version: "0.1.0".into(),
                protocol: PROTOCOL_VERSION,
                port: DEFAULT_PORT,
            },
        };
        let s = serde_json::to_string(&a).unwrap();
        let b: Announcement = serde_json::from_str(&s).unwrap();
        assert_eq!(b.info, a.info);
        assert_eq!(b.kind, AnnouncementKind::Announce);
    }
}
