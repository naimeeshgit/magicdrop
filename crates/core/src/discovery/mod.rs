//! Finding other MagicDrop devices on the local network.
//!
//! Two independent mechanisms run side by side, because networks block
//! different things:
//!
//! * **mDNS / DNS-SD** (`_magicdrop._tcp`, UDP 5353). The standard way; often
//!   whitelisted by VPNs and OS firewalls.
//! * **UDP multicast announce** (224.0.0.199:47471). Simple JSON; works where
//!   an existing mDNS responder (Bonjour, Windows' own) gets in the way.
//!
//! Every sighting lands in a [`PeerRegistry`], deduplicated by device id.
//! When we hear an announce we also call the sender's `/register` endpoint
//! over HTTP, so discovery still completes when only one direction of
//! multicast works.

mod mdns;
mod multicast;

use crate::error::Result;
use crate::net::InterfaceFilter;
use crate::peers::PeerRegistry;
use crate::protocol::{DeviceInfo, MULTICAST_PORT};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub filter: InterfaceFilter,
    pub enable_mdns: bool,
    pub enable_multicast: bool,
    pub multicast_port: u16,
    /// How often to re-announce over multicast.
    pub announce_interval: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            filter: InterfaceFilter::default(),
            enable_mdns: true,
            enable_multicast: true,
            multicast_port: MULTICAST_PORT,
            announce_interval: Duration::from_secs(20),
        }
    }
}

/// Running discovery. Drop or call [`Discovery::shutdown`] to stop.
pub struct Discovery {
    registry: PeerRegistry,
    mdns: Option<mdns::MdnsHandle>,
    multicast: Option<Arc<multicast::Multicast>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Discovery {
    /// Start advertising `me` and listening for others.
    ///
    /// Failures of one mechanism are logged and don't stop the other; this
    /// only errors if *both* fail to start.
    pub async fn start(
        me: DeviceInfo,
        registry: PeerRegistry,
        cfg: DiscoveryConfig,
    ) -> Result<Self> {
        let mut tasks = Vec::new();

        let mdns = if cfg.enable_mdns {
            match mdns::MdnsHandle::start(&me, registry.clone(), &cfg.filter) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!("mDNS discovery unavailable: {e}");
                    None
                }
            }
        } else {
            None
        };

        let multicast = if cfg.enable_multicast {
            match multicast::Multicast::start(
                me.clone(),
                registry.clone(),
                cfg.filter.clone(),
                cfg.multicast_port,
            ) {
                Ok((mc, recv_task)) => {
                    tasks.push(recv_task);
                    let mc = Arc::new(mc);
                    let mc2 = mc.clone();
                    let every = cfg.announce_interval;
                    tasks.push(tokio::spawn(async move {
                        // A few quick announces at startup (packets get lost on
                        // Wi-Fi), then the regular cadence.
                        for delay in [0u64, 500, 1500] {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                            mc2.announce().await;
                        }
                        let mut tick = tokio::time::interval(every);
                        tick.tick().await;
                        loop {
                            tick.tick().await;
                            mc2.announce().await;
                        }
                    }));
                    Some(mc)
                }
                Err(e) => {
                    tracing::warn!("multicast discovery unavailable: {e}");
                    None
                }
            }
        } else {
            None
        };

        if cfg.enable_mdns && cfg.enable_multicast && mdns.is_none() && multicast.is_none() {
            return Err(crate::Error::Other(
                "neither mDNS nor multicast discovery could start".into(),
            ));
        }

        let reg = registry.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            loop {
                tick.tick().await;
                reg.expire();
            }
        }));

        Ok(Self {
            registry,
            mdns,
            multicast,
            tasks,
        })
    }

    pub fn registry(&self) -> &PeerRegistry {
        &self.registry
    }

    /// Re-announce right now (e.g. when the user opens the device list).
    pub async fn refresh(&self) {
        if let Some(mc) = &self.multicast {
            mc.announce().await;
        }
        if let Some(m) = &self.mdns {
            m.refresh();
        }
    }

    /// Which mechanisms are running: (mdns, multicast).
    pub fn status(&self) -> (bool, bool) {
        (self.mdns.is_some(), self.multicast.is_some())
    }

    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        if let Some(m) = self.mdns.take() {
            m.shutdown();
        }
        self.multicast = None;
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.stop();
    }
}
