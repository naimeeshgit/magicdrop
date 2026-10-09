//! Shared networking setup for the CLI, plus `doctor`.

use anyhow::Result;
use magicdrop_core::discovery::{Discovery, DiscoveryConfig};
use magicdrop_core::net::{lan_interfaces, looks_virtual, InterfaceFilter};
use magicdrop_core::peers::PeerRegistry;
use magicdrop_core::protocol::{DEFAULT_PORT, MULTICAST_PORT};
use magicdrop_core::server::{self, ReceiveEvent, RunningServer, ServerConfig};
use magicdrop_core::Identity;
use std::time::Duration;
use tokio::sync::mpsc;

/// A running node: server (for `/info` and `/register`) plus discovery.
pub struct Node {
    pub server: RunningServer,
    pub events: mpsc::Receiver<ReceiveEvent>,
    pub discovery: Discovery,
    pub registry: PeerRegistry,
}

pub async fn start_node(identity: &Identity, cfg: ServerConfig) -> Result<Node> {
    let registry = PeerRegistry::new(identity.id.clone());
    let (server, events) = server::start(identity, cfg, Some(registry.clone())).await?;
    let discovery = Discovery::start(
        server.info.clone(),
        registry.clone(),
        DiscoveryConfig::default(),
    )
    .await?;
    Ok(Node {
        server,
        events,
        discovery,
        registry,
    })
}

/// Non-loopback addresses others can use to reach us, as "ip:port".
pub fn reachable_addresses(port: u16) -> Vec<String> {
    lan_interfaces(&InterfaceFilter::default())
        .into_iter()
        .filter(|i| i.ip.is_ipv4())
        .map(|i| format!("{}:{port}", i.ip))
        .collect()
}

pub fn troubleshooting_tips() -> &'static str {
    "If the other device doesn't show up:\n  \
     • Both devices must be on the same Wi-Fi, and the other one must be running `magicdrop receive`.\n  \
     • Campus, office and hotel Wi-Fi often block devices from seeing each other.\n    \
       Fix: turn on a phone hotspot and connect both laptops to it.\n  \
     • Windows: allow MagicDrop when the firewall asks, and set the Wi-Fi to \"Private\".\n  \
     • Pause any VPN.\n  \
     • You can always send by address: magicdrop send FILE --to <their IP>\n  \
     • Run `magicdrop doctor` on both devices for details."
}

pub async fn doctor(identity: &Identity) -> Result<()> {
    println!(
        "MagicDrop {} on {}",
        magicdrop_core::APP_VERSION,
        std::env::consts::OS
    );
    println!("Device name: {}", identity.alias);
    println!("Device id:   {}", &identity.id[..16]);
    println!("Settings:    {}", identity.home.display());
    println!();

    println!("Network interfaces:");
    let all = lan_interfaces(&InterfaceFilter {
        include_loopback: false,
        include_virtual: true,
    });
    if all.is_empty() {
        println!("  ✗ none — you're not connected to any network");
    }
    let mut usable = 0;
    for i in &all {
        let virt = looks_virtual(&i.name);
        if !virt && i.ip.is_ipv4() {
            usable += 1;
        }
        println!(
            "  {} {:<28} {}{}",
            if virt { "·" } else { "✓" },
            i.name,
            i.ip,
            if virt { "  (virtual/VPN, ignored)" } else { "" }
        );
    }
    if usable == 0 && !all.is_empty() {
        println!("  ✗ no usable Wi-Fi/Ethernet IPv4 address");
    }
    println!();

    println!("Ports:");
    for (port, what) in [
        (DEFAULT_PORT, "TCP transfers"),
        (MULTICAST_PORT, "UDP discovery"),
    ] {
        let ok = if port == DEFAULT_PORT {
            std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
        } else {
            std::net::UdpSocket::bind(("0.0.0.0", port)).is_ok()
        };
        println!(
            "  {} {port} ({what}){}",
            if ok { "✓" } else { "·" },
            if ok {
                ""
            } else {
                "  in use (fine if MagicDrop is already running; otherwise a random port is used)"
            }
        );
    }
    println!();

    println!("Looking for devices for 5 seconds…");
    let mut cfg = ServerConfig::new(server::default_download_dir());
    cfg.port = 0;
    cfg.accept_transfers = false;
    let node = start_node(identity, cfg).await?;
    let (mdns, mc) = node.discovery.status();
    println!(
        "  {} mDNS discovery\n  {} multicast discovery",
        if mdns { "✓" } else { "✗" },
        if mc { "✓" } else { "✗" }
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    let peers = node.registry.list();
    if peers.is_empty() {
        println!("  · no devices found\n");
        println!("{}", troubleshooting_tips());
    } else {
        for p in peers {
            println!(
                "  ✓ {} ({}) at {}",
                p.info.alias,
                p.info.os,
                p.addrs
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    if cfg!(windows) {
        println!(
            "\nWindows tip: if others can't see this PC, open Settings → Network & internet → Wi-Fi →\n\
             your network → set \"Network profile type\" to Private, and allow MagicDrop in the firewall."
        );
    }
    Ok(())
}
