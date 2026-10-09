//! Two devices on one machine find each other over multicast.
//!
//! Needs a network interface with multicast; CI runners have one. Set
//! MAGICDROP_SKIP_NET_TESTS=1 to skip on locked-down machines.

use magicdrop_core::discovery::{Discovery, DiscoveryConfig};
use magicdrop_core::net::InterfaceFilter;
use magicdrop_core::peers::PeerRegistry;
use magicdrop_core::server::{self, ServerConfig};
use magicdrop_core::Identity;
use std::time::Duration;

fn skip() -> bool {
    std::env::var_os("MAGICDROP_SKIP_NET_TESTS").is_some()
}

struct Node {
    _home: tempfile::TempDir,
    _dl: tempfile::TempDir,
    _server: server::RunningServer,
    id: String,
    discovery: Discovery,
}

async fn node(mc_port: Option<u16>, mdns: bool) -> Node {
    let home = tempfile::tempdir().unwrap();
    let dl = tempfile::tempdir().unwrap();
    let identity = Identity::load_or_create(home.path()).unwrap();
    let registry = PeerRegistry::new(identity.id.clone());
    let mut cfg = ServerConfig::new(dl.path());
    cfg.port = 0;
    let (server, mut events) = server::start(&identity, cfg, Some(registry.clone()))
        .await
        .unwrap();
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    let discovery = Discovery::start(
        server.info.clone(),
        registry,
        DiscoveryConfig {
            filter: InterfaceFilter {
                include_loopback: true,
                include_virtual: true,
            },
            enable_mdns: mdns,
            enable_multicast: mc_port.is_some(),
            multicast_port: mc_port.unwrap_or(0),
            announce_interval: Duration::from_secs(1),
        },
    )
    .await
    .unwrap();
    Node {
        _home: home,
        _dl: dl,
        _server: server,
        id: identity.id,
        discovery,
    }
}

async fn wait_for(n: &Node, id: &str) -> bool {
    for _ in 0..80 {
        if n.discovery.registry().get(id).is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_discover_each_other_via_multicast() {
    if skip() {
        return;
    }
    // Unique port per test run so parallel runs don't interfere.
    let port = 48000 + (std::process::id() % 1000) as u16;
    let a = node(Some(port), false).await;
    let b = node(Some(port), false).await;
    assert!(wait_for(&a, &b.id).await, "A never saw B");
    assert!(wait_for(&b, &a.id).await, "B never saw A");

    // The address we found is actually usable.
    let peer = a.discovery.registry().get(&b.id).unwrap();
    let addr = magicdrop_core::client::connect(&peer).await.unwrap();
    let info = magicdrop_core::client::info(addr).await.unwrap();
    assert_eq!(info.id, b.id);
}

/// mDNS on CI runners is unreliable (shared 5353, no multicast routing on some
/// images), so this one runs on demand: `cargo test -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn two_nodes_discover_each_other_via_mdns() {
    // Multicast disabled, so only mDNS can succeed.
    let a = node(None, true).await;
    let b = node(None, true).await;
    assert!(wait_for(&a, &b.id).await, "A never saw B over mDNS");
    assert!(wait_for(&b, &a.id).await, "B never saw A over mDNS");
}
