//! Persistent device identity.
//!
//! Each install generates a self-signed certificate once. The SHA-256 of its
//! DER bytes is the device id. Phase 3 will use the same certificate for
//! mutual TLS, so the id peers see today is the one they'll pin later.

use crate::error::{Error, Result};
use crate::protocol::{DeviceInfo, DeviceType, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const CERT_DER: &str = "device-cert.der";
const CERT_PEM: &str = "device-cert.pem";
const KEY_PEM: &str = "device-key.pem";
const SETTINGS: &str = "identity.json";

/// Environment variable to override where state is kept. Useful for running
/// two instances on one machine.
pub const HOME_ENV: &str = "MAGICDROP_HOME";

#[derive(Debug, Clone)]
pub struct Identity {
    pub id: String,
    pub alias: String,
    pub device_type: DeviceType,
    pub cert_der: Vec<u8>,
    pub cert_pem: String,
    pub key_pem: String,
    pub home: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct Settings {
    alias: String,
}

impl Identity {
    /// Where MagicDrop keeps its state: `$MAGICDROP_HOME` or the OS config dir.
    pub fn default_home() -> PathBuf {
        if let Some(h) = std::env::var_os(HOME_ENV) {
            return PathBuf::from(h);
        }
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("MagicDrop")
    }

    /// Load the identity from `home`, creating it on first run.
    pub fn load_or_create(home: &Path) -> Result<Self> {
        std::fs::create_dir_all(home)?;
        let der_path = home.join(CERT_DER);
        let pem_path = home.join(CERT_PEM);
        let key_path = home.join(KEY_PEM);

        let (cert_der, cert_pem, key_pem) =
            if der_path.exists() && pem_path.exists() && key_path.exists() {
                (
                    std::fs::read(&der_path)?,
                    std::fs::read_to_string(&pem_path)?,
                    std::fs::read_to_string(&key_path)?,
                )
            } else {
                let ck = rcgen::generate_simple_self_signed(vec!["magicdrop.local".to_string()])
                    .map_err(|e| Error::Identity(e.to_string()))?;
                let der = ck.cert.der().to_vec();
                let pem = ck.cert.pem();
                let key = ck.key_pair.serialize_pem();
                // Write the key first and the DER last: DER's presence marks a
                // complete identity.
                write_private(&key_path, key.as_bytes())?;
                std::fs::write(&pem_path, &pem)?;
                std::fs::write(&der_path, &der)?;
                (der, pem, key)
            };

        let id = fingerprint(&cert_der);

        let settings_path = home.join(SETTINGS);
        let alias = match std::fs::read(&settings_path)
            .ok()
            .and_then(|b| serde_json::from_slice::<Settings>(&b).ok())
        {
            Some(s) if !s.alias.trim().is_empty() => s.alias,
            _ => {
                let alias = default_alias();
                let s = Settings {
                    alias: alias.clone(),
                };
                std::fs::write(&settings_path, serde_json::to_vec_pretty(&s)?)?;
                alias
            }
        };

        Ok(Self {
            id,
            alias,
            device_type: guess_device_type(),
            cert_der,
            cert_pem,
            key_pem,
            home: home.to_path_buf(),
        })
    }

    /// Change and persist the display name.
    pub fn set_alias(&mut self, alias: &str) -> Result<()> {
        let alias = clean_alias(alias);
        if alias.is_empty() {
            return Err(Error::Other("name can't be empty".into()));
        }
        let s = Settings {
            alias: alias.clone(),
        };
        std::fs::write(self.home.join(SETTINGS), serde_json::to_vec_pretty(&s)?)?;
        self.alias = alias;
        Ok(())
    }

    /// The public description of this device, for the given HTTP port.
    pub fn device_info(&self, port: u16) -> DeviceInfo {
        DeviceInfo {
            id: self.id.clone(),
            alias: self.alias.clone(),
            device_type: self.device_type,
            os: std::env::consts::OS.to_string(),
            version: crate::APP_VERSION.to_string(),
            protocol: PROTOCOL_VERSION,
            port,
        }
    }
}

/// Lowercase hex SHA-256 of a certificate's DER bytes.
pub fn fingerprint(der: &[u8]) -> String {
    hex::encode(Sha256::digest(der))
}

fn default_alias() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_default();
    let host = clean_alias(host.trim_end_matches(".local"));
    if host.is_empty() {
        "MagicDrop device".to_string()
    } else {
        host
    }
}

/// Strip control characters and limit length so names are safe to display
/// and fit in an mDNS TXT record.
pub fn clean_alias(s: &str) -> String {
    let cleaned: String = s.chars().filter(|c| !c.is_control()).collect();
    let trimmed = cleaned.trim();
    trimmed.chars().take(48).collect()
}

fn guess_device_type() -> DeviceType {
    // Without platform APIs we can't tell laptop from desktop reliably; "laptop"
    // is the common case for this app and only affects the icon.
    match std::env::consts::OS {
        "windows" | "macos" | "linux" => DeviceType::Laptop,
        "android" | "ios" => DeviceType::Phone,
        _ => DeviceType::Other,
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    // On Windows the per-user config dir is already private to the user.
    std::fs::write(path, bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_across_loads() {
        let dir = tempfile::tempdir().unwrap();
        let a = Identity::load_or_create(dir.path()).unwrap();
        let b = Identity::load_or_create(dir.path()).unwrap();
        assert_eq!(a.id, b.id);
        assert_eq!(a.id.len(), 64);
        assert_eq!(a.alias, b.alias);
    }

    #[test]
    fn different_homes_get_different_ids() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let a = Identity::load_or_create(d1.path()).unwrap();
        let b = Identity::load_or_create(d2.path()).unwrap();
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn alias_persists() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = Identity::load_or_create(dir.path()).unwrap();
        a.set_alias("  Time's\u{7} Laptop ").unwrap();
        let b = Identity::load_or_create(dir.path()).unwrap();
        assert_eq!(b.alias, "Time's Laptop");
    }
}
