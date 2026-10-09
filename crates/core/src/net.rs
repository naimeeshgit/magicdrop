//! Network interface selection.
//!
//! Machines often have virtual adapters (WSL, Hyper-V, Docker, VirtualBox,
//! VPN tunnels). Announcing on those wastes packets and, worse, can make a
//! peer try an address it can't reach. We keep only real LAN interfaces.

use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanInterface {
    pub name: String,
    pub ip: IpAddr,
    pub index: Option<u32>,
}

impl LanInterface {
    pub fn ipv4(&self) -> Option<Ipv4Addr> {
        match self.ip {
            IpAddr::V4(v4) => Some(v4),
            IpAddr::V6(_) => None,
        }
    }
}

/// Name fragments that identify virtual / tunnel adapters (matched lowercase).
const VIRTUAL_HINTS: &[&str] = &[
    "vethernet",
    "virtualbox",
    "vboxnet",
    "vmware",
    "vmnet",
    "hyper-v",
    "docker",
    "br-",
    "veth",
    "virbr",
    "wsl",
    "tailscale",
    "zerotier",
    "utun",
    "tun",
    "tap",
    "wg",
    "wireguard",
    "nordlynx",
    "proton",
    "openvpn",
    "llw",
    "awdl",
    "anpi",
    "bridge",
    "loopback pseudo",
];

/// True if the adapter name looks like a virtual/VPN adapter.
pub fn looks_virtual(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    VIRTUAL_HINTS.iter().any(|h| {
        // "tun"/"tap"/"wg" must be a prefix to avoid matching e.g. "ethernet-tunable".
        if matches!(*h, "tun" | "tap" | "wg" | "br-" | "veth") {
            n.starts_with(h)
        } else {
            n.contains(h)
        }
    })
}

/// Options for interface selection.
#[derive(Debug, Clone, Default)]
pub struct InterfaceFilter {
    /// Include loopback (tests, single-machine demos).
    pub include_loopback: bool,
    /// Include adapters that look virtual.
    pub include_virtual: bool,
}

/// List usable LAN interfaces (IPv4 and IPv6), best candidates first.
pub fn lan_interfaces(filter: &InterfaceFilter) -> Vec<LanInterface> {
    let all = match if_addrs::get_if_addrs() {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("could not list network interfaces: {e}");
            return Vec::new();
        }
    };
    let mut out: Vec<LanInterface> = all
        .into_iter()
        .filter(|i| i.is_oper_up() || i.is_loopback())
        .filter(|i| filter.include_loopback || !i.is_loopback())
        .filter(|i| filter.include_virtual || !looks_virtual(&i.name))
        .filter(|i| match i.ip() {
            // 169.254/16 means DHCP failed; not useful.
            IpAddr::V4(v4) => !v4.is_link_local() || i.is_loopback(),
            IpAddr::V6(_) => true,
        })
        .map(|i| LanInterface {
            ip: i.ip(),
            index: i.index,
            name: i.name,
        })
        .collect();
    out.sort_by_key(|i| (i.ip.is_loopback(), i.ip.is_ipv6(), !is_private(&i.ip)));
    out.dedup();
    out
}

/// IPv4-only convenience.
pub fn lan_ipv4s(filter: &InterfaceFilter) -> Vec<Ipv4Addr> {
    let mut v: Vec<Ipv4Addr> = lan_interfaces(filter)
        .iter()
        .filter_map(|i| i.ipv4())
        .collect();
    v.dedup();
    v
}

/// RFC1918 private IPv4 or IPv6 ULA / link-local.
pub fn is_private(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private(),
        IpAddr::V6(v6) => {
            let s = v6.segments()[0];
            (s & 0xfe00) == 0xfc00 || (s & 0xffc0) == 0xfe80
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_adapter_detection() {
        assert!(looks_virtual("vEthernet (WSL)"));
        assert!(looks_virtual("VirtualBox Host-Only Network"));
        assert!(looks_virtual("docker0"));
        assert!(looks_virtual("utun3"));
        assert!(looks_virtual("tun0"));
        assert!(looks_virtual("Tailscale"));
        assert!(looks_virtual("wg0"));
        assert!(!looks_virtual("Wi-Fi"));
        assert!(!looks_virtual("Ethernet"));
        assert!(!looks_virtual("en0"));
        assert!(!looks_virtual("wlan0"));
        assert!(!looks_virtual("eth0"));
    }

    #[test]
    fn private_ranges() {
        assert!(is_private(&"192.168.1.5".parse().unwrap()));
        assert!(is_private(&"10.0.0.1".parse().unwrap()));
        assert!(!is_private(&"8.8.8.8".parse().unwrap()));
        assert!(is_private(&"fe80::1".parse().unwrap()));
        assert!(is_private(&"fd00::1".parse().unwrap()));
    }

    #[test]
    fn listing_does_not_panic() {
        let _ = lan_interfaces(&InterfaceFilter::default());
        let lo = lan_interfaces(&InterfaceFilter {
            include_loopback: true,
            include_virtual: true,
        });
        assert!(lo.iter().any(|i| i.ip.is_loopback()));
    }
}
