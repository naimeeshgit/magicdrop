use crate::error::Result;
use crate::net::{lan_ipv4s, InterfaceFilter};
use crate::peers::{PeerRegistry, Source};
use crate::protocol::{
    Announcement, AnnouncementKind, DeviceInfo, MULTICAST_GROUP_V4, PROTOCOL_VERSION,
};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Mutex;
use tokio::net::UdpSocket;

const MAX_PACKET: usize = 1400;

pub(super) struct Multicast {
    sock: std::sync::Arc<UdpSocket>,
    me: DeviceInfo,
    filter: InterfaceFilter,
    port: u16,
    joined: Mutex<HashSet<Ipv4Addr>>,
    /// Serialises announces: each one switches the socket's outgoing
    /// interface, so two at once could send on the wrong interface.
    send_lock: tokio::sync::Mutex<()>,
}

impl Multicast {
    pub(super) fn start(
        me: DeviceInfo,
        registry: PeerRegistry,
        filter: InterfaceFilter,
        port: u16,
    ) -> Result<(Self, tokio::task::JoinHandle<()>)> {
        let std_sock = bind_socket(port)?;
        let sock = std::sync::Arc::new(UdpSocket::from_std(std_sock)?);
        let mc = Self {
            sock: sock.clone(),
            me: me.clone(),
            filter,
            port,
            joined: Mutex::new(HashSet::new()),
            send_lock: tokio::sync::Mutex::new(()),
        };
        mc.join_groups();

        let task = tokio::spawn(recv_loop(sock, me, registry, port));
        Ok((mc, task))
    }

    /// Join the group on any interface we haven't joined yet. Called on every
    /// announce, so a laptop that switches Wi-Fi picks up the new interface.
    fn join_groups(&self) {
        let ifaces = lan_ipv4s(&self.filter);
        let mut joined = self.joined.lock().unwrap();
        for ip in ifaces {
            if joined.contains(&ip) {
                continue;
            }
            match self.sock.join_multicast_v4(MULTICAST_GROUP_V4, ip) {
                Ok(()) => {
                    tracing::debug!("joined multicast group on {ip}");
                    joined.insert(ip);
                }
                Err(e) => tracing::debug!("join on {ip} failed: {e}"),
            }
        }
    }

    pub(super) async fn announce(&self) {
        let _guard = self.send_lock.lock().await;
        self.join_groups();
        let payload = encode(&self.me, AnnouncementKind::Announce);
        let target = SocketAddr::V4(SocketAddrV4::new(MULTICAST_GROUP_V4, self.port));
        let ifaces = lan_ipv4s(&self.filter);
        if ifaces.is_empty() {
            tracing::debug!("no LAN interfaces to announce on");
        }
        for ip in ifaces {
            // Pick the outgoing interface, then send. Done sequentially on one
            // socket, so the interface choice can't race.
            if let Err(e) = SockRef::from(&*self.sock).set_multicast_if_v4(&ip) {
                tracing::debug!("set multicast if {ip}: {e}");
                continue;
            }
            if let Err(e) = self.sock.send_to(&payload, target).await {
                tracing::debug!("announce via {ip} failed: {e}");
            }
        }
    }
}

fn bind_socket(port: u16) -> std::io::Result<std::net::UdpSocket> {
    let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    s.set_reuse_address(true)?;
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    s.set_reuse_port(true)?;
    s.set_multicast_loop_v4(true)?;
    s.set_multicast_ttl_v4(1)?;
    s.bind(&SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).into())?;
    s.set_nonblocking(true)?;
    Ok(s.into())
}

fn encode(me: &DeviceInfo, kind: AnnouncementKind) -> Vec<u8> {
    serde_json::to_vec(&Announcement {
        magicdrop: PROTOCOL_VERSION,
        kind,
        info: me.clone(),
    })
    .expect("announcement serializes")
}

async fn recv_loop(
    sock: std::sync::Arc<UdpSocket>,
    me: DeviceInfo,
    registry: PeerRegistry,
    port: u16,
) {
    let mut buf = vec![0u8; 4096];
    loop {
        let (n, src) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                // Windows reports ICMP "port unreachable" from earlier sends as
                // recv errors (WSAECONNRESET). Harmless; keep going.
                tracing::trace!("multicast recv error: {e}");
                continue;
            }
        };
        if n > MAX_PACKET * 2 {
            continue;
        }
        let Ok(ann) = serde_json::from_slice::<Announcement>(&buf[..n]) else {
            continue;
        };
        if ann.magicdrop != PROTOCOL_VERSION
            || ann.info.protocol != PROTOCOL_VERSION
            || ann.info.id == me.id
        {
            continue;
        }
        let mut info = ann.info;
        info.alias = crate::identity::clean_alias(&info.alias);
        let ip = src.ip();
        registry.upsert(info.clone(), ip, Source::Multicast);

        if ann.kind == AnnouncementKind::Announce {
            // 1) Unicast UDP reply straight back.
            let reply = encode(&me, AnnouncementKind::Reply);
            let _ = sock.send_to(&reply, SocketAddr::new(ip, port)).await;
            // 2) HTTP /register, which also tells them our address from their
            //    side and gets through when UDP replies are filtered.
            let me2 = me.clone();
            let reg = registry.clone();
            tokio::spawn(async move {
                let addr = SocketAddr::new(ip, info.port);
                match crate::client::register(addr, &me2).await {
                    Ok(their) if their.id == info.id => {
                        reg.upsert(their, ip, Source::Register);
                    }
                    Ok(_) => tracing::debug!("register at {addr}: id mismatch"),
                    Err(e) => tracing::debug!("register at {addr} failed: {e}"),
                }
            });
        }
    }
}
