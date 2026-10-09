//! Registry of devices seen on the network.
//!
//! Discovery sources (mDNS, multicast, `/register` callbacks, manual entry) all
//! feed into one registry. Entries are keyed by device id, so the same laptop
//! seen through two mechanisms or two addresses shows up once.

use crate::protocol::DeviceInfo;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// How long a peer stays listed without being heard from.
pub const PEER_TTL: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    Mdns,
    Multicast,
    Register,
    Manual,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub info: DeviceInfo,
    /// Known addresses, most recently confirmed first.
    pub addrs: Vec<IpAddr>,
    pub last_seen: Instant,
    pub sources: Vec<Source>,
}

impl Peer {
    /// Socket addresses to try, in order.
    pub fn endpoints(&self) -> Vec<std::net::SocketAddr> {
        self.addrs
            .iter()
            .map(|ip| std::net::SocketAddr::new(*ip, self.info.port))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// New device, or a known device changed (name, port, addresses).
    Updated(Peer),
    /// Device went away (mDNS goodbye or TTL expiry).
    Removed(String),
}

#[derive(Clone)]
pub struct PeerRegistry {
    inner: Arc<Mutex<HashMap<String, Peer>>>,
    my_id: String,
    events: broadcast::Sender<PeerEvent>,
}

impl PeerRegistry {
    pub fn new(my_id: impl Into<String>) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            inner: Arc::default(),
            my_id: my_id.into(),
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.events.subscribe()
    }

    /// Record a sighting. Returns true if this was new information.
    pub fn upsert(&self, info: DeviceInfo, addr: IpAddr, source: Source) -> bool {
        if info.id == self.my_id || info.id.is_empty() {
            return false;
        }
        let mut map = self.inner.lock().unwrap();
        let now = Instant::now();
        let (changed, snapshot) = match map.get_mut(&info.id) {
            Some(p) => {
                let mut changed = p.info != info;
                p.info = info;
                p.last_seen = now;
                if let Some(pos) = p.addrs.iter().position(|a| *a == addr) {
                    // Move to front: most recently confirmed.
                    if pos != 0 {
                        let a = p.addrs.remove(pos);
                        p.addrs.insert(0, a);
                    }
                } else {
                    p.addrs.insert(0, addr);
                    p.addrs.truncate(8);
                    changed = true;
                }
                if !p.sources.contains(&source) {
                    p.sources.push(source);
                }
                (changed, p.clone())
            }
            None => {
                let p = Peer {
                    info: info.clone(),
                    addrs: vec![addr],
                    last_seen: now,
                    sources: vec![source],
                };
                map.insert(info.id.clone(), p.clone());
                (true, p)
            }
        };
        drop(map);
        if changed {
            let _ = self.events.send(PeerEvent::Updated(snapshot));
        }
        changed
    }

    pub fn remove(&self, id: &str) {
        if self.inner.lock().unwrap().remove(id).is_some() {
            let _ = self.events.send(PeerEvent::Removed(id.to_string()));
        }
    }

    /// Drop peers not heard from within [`PEER_TTL`].
    pub fn expire(&self) {
        let now = Instant::now();
        let gone: Vec<String> = {
            let mut map = self.inner.lock().unwrap();
            let gone: Vec<String> = map
                .iter()
                .filter(|(_, p)| now.duration_since(p.last_seen) > PEER_TTL)
                .map(|(k, _)| k.clone())
                .collect();
            for k in &gone {
                map.remove(k);
            }
            gone
        };
        for id in gone {
            let _ = self.events.send(PeerEvent::Removed(id));
        }
    }

    /// Current peers sorted by name.
    pub fn list(&self) -> Vec<Peer> {
        let mut v: Vec<Peer> = self.inner.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| {
            a.info
                .alias
                .to_lowercase()
                .cmp(&b.info.alias.to_lowercase())
                .then(a.info.id.cmp(&b.info.id))
        });
        v
    }

    pub fn get(&self, id: &str) -> Option<Peer> {
        self.inner.lock().unwrap().get(id).cloned()
    }

    /// Find peers by name (case-insensitive exact, then prefix) or id prefix.
    pub fn find(&self, query: &str) -> Vec<Peer> {
        let q = query.trim().to_lowercase();
        let all = self.list();
        let exact: Vec<Peer> = all
            .iter()
            .filter(|p| p.info.alias.to_lowercase() == q || p.info.id.starts_with(&q))
            .cloned()
            .collect();
        if !exact.is_empty() {
            return exact;
        }
        all.into_iter()
            .filter(|p| p.info.alias.to_lowercase().contains(&q))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DeviceType, PROTOCOL_VERSION};

    fn dev(id: &str, alias: &str) -> DeviceInfo {
        DeviceInfo {
            id: id.into(),
            alias: alias.into(),
            device_type: DeviceType::Laptop,
            os: "linux".into(),
            version: "0".into(),
            protocol: PROTOCOL_VERSION,
            port: 1,
        }
    }

    #[test]
    fn dedupes_by_id_and_tracks_addresses() {
        let r = PeerRegistry::new("me");
        let a: IpAddr = "192.168.1.2".parse().unwrap();
        let b: IpAddr = "192.168.1.3".parse().unwrap();
        assert!(r.upsert(dev("x", "Laptop"), a, Source::Mdns));
        assert!(!r.upsert(dev("x", "Laptop"), a, Source::Multicast));
        assert!(r.upsert(dev("x", "Laptop"), b, Source::Multicast));
        let l = r.list();
        assert_eq!(l.len(), 1);
        assert_eq!(l[0].addrs, vec![b, a]);
        assert_eq!(l[0].sources, vec![Source::Mdns, Source::Multicast]);
        // Re-confirming `a` moves it to the front.
        r.upsert(dev("x", "Laptop"), a, Source::Mdns);
        assert_eq!(r.get("x").unwrap().addrs, vec![a, b]);
    }

    #[test]
    fn ignores_self() {
        let r = PeerRegistry::new("me");
        assert!(!r.upsert(dev("me", "Me"), "10.0.0.1".parse().unwrap(), Source::Mdns));
        assert!(r.list().is_empty());
    }

    #[test]
    fn find_by_name_or_id() {
        let r = PeerRegistry::new("me");
        let ip: IpAddr = "10.0.0.2".parse().unwrap();
        r.upsert(dev("abcdef", "Riya's MacBook"), ip, Source::Mdns);
        r.upsert(dev("123456", "Time's Laptop"), ip, Source::Mdns);
        assert_eq!(r.find("riya's macbook").len(), 1);
        assert_eq!(r.find("abc").len(), 1);
        assert_eq!(r.find("mac").len(), 1);
        assert_eq!(r.find("'s").len(), 2);
        assert!(r.find("pixel").is_empty());
    }

    #[test]
    fn emits_events() {
        let r = PeerRegistry::new("me");
        let mut rx = r.subscribe();
        r.upsert(dev("x", "X"), "10.0.0.2".parse().unwrap(), Source::Mdns);
        assert!(matches!(rx.try_recv().unwrap(), PeerEvent::Updated(_)));
        r.remove("x");
        assert!(matches!(rx.try_recv().unwrap(), PeerEvent::Removed(id) if id == "x"));
    }
}
