use crate::error::Result;
use crate::net::{looks_virtual, InterfaceFilter};
use crate::peers::{PeerRegistry, Source};
use crate::protocol::{DeviceInfo, DeviceType, MDNS_SERVICE_TYPE, PROTOCOL_VERSION};
use mdns_sd::{IfKind, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use std::net::IpAddr;

pub(super) struct MdnsHandle {
    daemon: ServiceDaemon,
    browse_task: tokio::task::JoinHandle<()>,
}

impl MdnsHandle {
    pub(super) fn start(
        me: &DeviceInfo,
        registry: PeerRegistry,
        filter: &InterfaceFilter,
    ) -> Result<Self> {
        let daemon = ServiceDaemon::new()?;

        if !filter.include_virtual {
            if let Ok(all) = if_addrs::get_if_addrs() {
                let mut seen = std::collections::HashSet::new();
                for i in all {
                    if looks_virtual(&i.name) && seen.insert(i.name.clone()) {
                        let _ = daemon.disable_interface(IfKind::Name(i.name));
                    }
                }
            }
        }
        if !filter.include_loopback {
            let _ = daemon.disable_interface(vec![IfKind::LoopbackV4, IfKind::LoopbackV6]);
        }

        let instance = format!("magicdrop-{}", &me.id[..12.min(me.id.len())]);
        let host = format!("{instance}.local.");
        let device_type = serde_json::to_value(me.device_type)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "other".into());
        let props = [
            ("id", me.id.as_str()),
            ("alias", me.alias.as_str()),
            ("type", device_type.as_str()),
            ("os", me.os.as_str()),
            ("ver", me.version.as_str()),
            ("proto", &me.protocol.to_string()),
        ];
        let info = ServiceInfo::new(MDNS_SERVICE_TYPE, &instance, &host, "", me.port, &props[..])?
            .enable_addr_auto();
        daemon.register(info)?;

        let rx = daemon.browse(MDNS_SERVICE_TYPE)?;
        let my_id = me.id.clone();
        let browse_task = tokio::spawn(async move {
            // fullname -> device id, so removals can be mapped back.
            let mut names: HashMap<String, String> = HashMap::new();
            while let Ok(event) = rx.recv_async().await {
                match event {
                    ServiceEvent::ServiceResolved(svc) => {
                        let Some(info) = parse_txt(&svc) else {
                            continue;
                        };
                        if info.id == my_id {
                            continue;
                        }
                        names.insert(svc.fullname.clone(), info.id.clone());
                        // IPv4 first; IPv6 link-local needs scope ids, which
                        // we'll handle in the hotspot/IPv6 phase.
                        let mut addrs: Vec<IpAddr> = svc
                            .addresses
                            .iter()
                            .map(|a| a.to_ip_addr())
                            .filter(|a| match a {
                                IpAddr::V4(v4) => !v4.is_link_local(),
                                IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) != 0xfe80,
                            })
                            .collect();
                        addrs.sort_by_key(|a| a.is_ipv6());
                        // Insert in reverse so the best address ends up first.
                        for a in addrs.into_iter().rev() {
                            registry.upsert(info.clone(), a, Source::Mdns);
                        }
                    }
                    ServiceEvent::ServiceRemoved(_, fullname) => {
                        if let Some(id) = names.remove(&fullname) {
                            registry.remove(&id);
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(Self {
            daemon,
            browse_task,
        })
    }

    /// Ask the network again. mdns-sd re-queries on its own schedule; a fresh
    /// browse call nudges it.
    pub(super) fn refresh(&self) {
        let _ = self.daemon.browse(MDNS_SERVICE_TYPE);
    }

    pub(super) fn shutdown(self) {
        self.browse_task.abort();
        let _ = self.daemon.shutdown();
    }
}

fn parse_txt(svc: &mdns_sd::ResolvedService) -> Option<DeviceInfo> {
    let get = |k: &str| {
        svc.txt_properties
            .get_property_val_str(k)
            .map(str::to_string)
    };
    let id = get("id")?;
    if id.len() != 64 || !id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let protocol: u32 = get("proto")?.parse().ok()?;
    if protocol != PROTOCOL_VERSION {
        tracing::debug!("ignoring {} with protocol {protocol}", svc.fullname);
        return None;
    }
    let device_type: DeviceType = get("type")
        .and_then(|t| serde_json::from_value(serde_json::Value::String(t)).ok())
        .unwrap_or_default();
    Some(DeviceInfo {
        alias: crate::identity::clean_alias(&get("alias").unwrap_or_else(|| "Unknown".into())),
        id,
        device_type,
        os: get("os").unwrap_or_default(),
        version: get("ver").unwrap_or_default(),
        protocol,
        port: svc.port,
    })
}
