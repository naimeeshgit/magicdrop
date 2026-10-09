use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("network request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("mDNS error: {0}")]
    Mdns(#[from] mdns_sd::Error),

    #[error("could not create device identity: {0}")]
    Identity(String),

    /// The other side sent something that doesn't follow the protocol.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The receiver said no.
    #[error("the receiver declined the transfer")]
    Declined,

    /// The receiver is already busy with another transfer.
    #[error("the receiver is busy with another transfer")]
    Busy,

    /// The device is online but not in receive mode.
    #[error("the other device isn't receiving files right now")]
    NotReceiving,

    /// We couldn't reach the device at any of its known addresses.
    #[error("could not reach {0}")]
    Unreachable(String),

    /// Contacted a device but it's not the one we expected (id mismatch).
    #[error("device identity mismatch (expected {expected}, got {actual})")]
    IdentityMismatch { expected: String, actual: String },

    /// Data arrived but its checksum doesn't match what was sent.
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(String),

    /// A file or folder name that's unsafe to write to disk.
    #[error("unsafe file path: {0}")]
    UnsafePath(String),

    #[error("transfer cancelled")]
    Cancelled,

    #[error("not enough disk space: need {needed} bytes, have {available}")]
    DiskFull { needed: u64, available: u64 },

    #[error("{0}")]
    Other(String),
}
