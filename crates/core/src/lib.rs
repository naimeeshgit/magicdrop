//! MagicDrop core engine.
//!
//! Everything the app needs that isn't UI: device identity, LAN discovery
//! (mDNS + UDP multicast), the receive server, the send client, and the
//! filesystem safety rules for writing incoming files.
//!
//! The crate is UI-agnostic. Front-ends (the CLI today, the Tauri desktop
//! app later) talk to it through async functions and event channels.

pub mod client;
pub mod discovery;
pub mod error;
pub mod identity;
pub mod net;
pub mod paths;
pub mod peers;
pub mod protocol;
pub mod server;
pub mod util;

pub use error::{Error, Result};
pub use identity::Identity;
pub use protocol::DeviceInfo;

/// Version of this build, sent to peers so they can show it / adapt.
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
