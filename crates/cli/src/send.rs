use crate::net::{start_node, troubleshooting_tips, Node};
use anyhow::{anyhow, bail, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use magicdrop_core::client::CancellationToken;
use magicdrop_core::client::{self, collect_files, SendEvent};
use magicdrop_core::peers::Peer;
use magicdrop_core::protocol::DEFAULT_PORT;
use magicdrop_core::server::{self, ServerConfig};
use magicdrop_core::util::human_bytes;
use magicdrop_core::{Error, Identity};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Start a node that only does discovery (refuses incoming transfers).
async fn discovery_node(identity: &Identity) -> Result<Node> {
    let mut cfg = ServerConfig::new(server::default_download_dir());
    cfg.port = 0;
    cfg.accept_transfers = false;
    start_node(identity, cfg).await
}

/// Look for devices for up to `wait` seconds, stopping early if `done` says so.
async fn scan(node: &Node, wait: u64, mut done: impl FnMut(&[Peer]) -> bool) -> Vec<Peer> {
    let deadline = Instant::now() + Duration::from_secs(wait.max(1));
    let mut refreshed = false;
    loop {
        let peers = node.registry.list();
        if done(&peers) || Instant::now() >= deadline {
            return peers;
        }
        if !refreshed && Instant::now() + Duration::from_secs(2) >= deadline {
            node.discovery.refresh().await;
            refreshed = true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

fn spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} {msg}").expect("valid template"));
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(Duration::from_millis(90));
    pb
}

fn describe(p: &Peer) -> String {
    format!(
        "{}  ({}, {})",
        p.info.alias,
        if p.info.os.is_empty() {
            "?"
        } else {
            &p.info.os
        },
        p.addrs.first().map(|a| a.to_string()).unwrap_or_default()
    )
}

pub async fn list_devices(identity: &Identity, wait: u64) -> Result<()> {
    let node = discovery_node(identity).await?;
    let sp = spinner("Looking for devices…");
    let peers = scan(&node, wait, |_| false).await;
    sp.finish_and_clear();
    if peers.is_empty() {
        println!("No devices found.\n\n{}", troubleshooting_tips());
    } else {
        println!("Nearby devices:");
        for p in &peers {
            println!("  • {}", describe(p));
        }
    }
    Ok(())
}

/// Parse "1.2.3.4", "1.2.3.4:5000", "[fe80::1]:5000", "fe80::1".
fn parse_addr(s: &str) -> Option<SocketAddr> {
    if let Ok(sa) = s.parse::<SocketAddr>() {
        return Some(sa);
    }
    s.parse::<IpAddr>()
        .ok()
        .map(|ip| SocketAddr::new(ip, DEFAULT_PORT))
}

pub async fn run(
    identity: &Identity,
    paths: Vec<PathBuf>,
    to: Option<String>,
    wait: u64,
) -> Result<()> {
    let files = collect_files(&paths).context("couldn't read the files to send")?;
    let total: u64 = files.iter().map(|f| f.size).sum();
    let names: HashMap<String, String> = files
        .iter()
        .map(|f| (f.id.clone(), f.rel.clone()))
        .collect();

    let node = discovery_node(identity).await?;
    let me = node.server.info.clone();

    // ---- pick the target ----
    let (addr, target_name) = if let Some(addr) = to.as_deref().and_then(parse_addr) {
        let info = client::info(addr).await.map_err(|e| {
            anyhow!(
                "couldn't reach MagicDrop at {addr}: {e}\nIs `magicdrop receive` running there?"
            )
        })?;
        (addr, info.alias)
    } else {
        let peer = choose_peer(&node, to.as_deref(), wait).await?;
        let sp = spinner(&format!("Connecting to {}…", peer.info.alias));
        let addr = client::connect(&peer).await;
        sp.finish_and_clear();
        (addr?, peer.info.alias)
    };

    println!(
        "Sending {} file{} ({}) to {}.",
        files.len(),
        if files.len() == 1 { "" } else { "s" },
        human_bytes(total),
        target_name
    );

    // ---- send with progress ----
    let (tx, mut rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let task = {
        let cancel = cancel.clone();
        tokio::spawn(async move { client::send_files(addr, &me, files, tx, cancel).await })
    };

    let waiting = spinner(&format!("Waiting for {target_name} to accept…"));
    let mut waiting = Some(waiting);
    let mut pb: Option<ProgressBar> = None;
    let mut sent: HashMap<String, u64> = HashMap::new();
    let started = Instant::now();
    let mut failures = Vec::new();

    loop {
        let ev = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                cancel.cancel();
                if let Some(w) = waiting.take() { w.finish_and_clear(); }
                if let Some(p) = &pb { p.abandon(); }
                eprintln!("\nCancelling…");
                let _ = task.await;
                bail!("cancelled");
            }
            ev = rx.recv() => ev,
        };
        let Some(ev) = ev else { break };
        match ev {
            SendEvent::WaitingForAccept => {}
            SendEvent::Accepted => {
                if let Some(w) = waiting.take() {
                    w.finish_and_clear();
                }
                let bar = ProgressBar::new(total);
                bar.set_style(
                    ProgressStyle::with_template(
                        "  {bar:30.cyan/blue} {bytes}/{total_bytes}  {binary_bytes_per_sec}  {eta}  {msg}",
                    )
                    .expect("valid template")
                    .progress_chars("█▉▊▋▌▍▎▏ "),
                );
                pb = Some(bar);
            }
            SendEvent::FileStarted { file_id, .. } => {
                if let Some(p) = &pb {
                    p.set_message(names.get(&file_id).cloned().unwrap_or_default());
                }
            }
            SendEvent::FileProgress { file_id, sent: n } => {
                sent.insert(file_id, n);
                if let Some(p) = &pb {
                    p.set_position(sent.values().sum());
                }
            }
            SendEvent::FileDone { .. } => {}
            SendEvent::FileRetry { file_id, attempt } => {
                sent.insert(file_id.clone(), 0);
                if let Some(p) = &pb {
                    p.println(format!(
                        "  ↻ retrying {} (attempt {})",
                        names.get(&file_id).map(String::as_str).unwrap_or("file"),
                        attempt + 1
                    ));
                }
            }
            SendEvent::FileFailed { file_id, error } => {
                let name = names.get(&file_id).cloned().unwrap_or_default();
                if let Some(p) = &pb {
                    p.println(format!("  ✗ {name}: {error}"));
                }
                failures.push(name);
            }
        }
    }
    if let Some(w) = waiting.take() {
        w.finish_and_clear();
    }
    if let Some(p) = &pb {
        p.finish_and_clear();
    }

    match task.await? {
        Ok(summary) => {
            let secs = started.elapsed().as_secs_f64().max(0.001);
            if summary.failed == 0 {
                println!(
                    "✓ Sent {} file{} ({}) in {:.1}s — {}/s",
                    summary.sent,
                    if summary.sent == 1 { "" } else { "s" },
                    human_bytes(summary.bytes),
                    secs,
                    human_bytes((summary.bytes as f64 / secs) as u64)
                );
                Ok(())
            } else {
                bail!(
                    "sent {} file(s), {} failed: {}",
                    summary.sent,
                    summary.failed,
                    failures.join(", ")
                )
            }
        }
        Err(Error::Declined) => bail!("{target_name} declined the transfer"),
        Err(Error::Busy) => {
            bail!("{target_name} is busy with another transfer; try again in a moment")
        }
        Err(Error::NotReceiving) => {
            bail!("{target_name} isn't receiving. Ask them to run `magicdrop receive`.")
        }
        Err(e) => Err(e.into()),
    }
}

async fn choose_peer(node: &Node, query: Option<&str>, wait: u64) -> Result<Peer> {
    let sp = spinner(match query {
        Some(_) => "Looking for the device…",
        None => "Looking for nearby devices…",
    });
    let peers = match query {
        Some(q) => {
            let q = q.to_string();
            let reg = node.registry.clone();
            scan(node, wait, move |_| reg.find(&q).len() == 1).await;
            node.registry.find(query.unwrap())
        }
        None => scan(node, wait, |_| false).await,
    };
    sp.finish_and_clear();

    match peers.len() {
        0 => {
            let what = match query {
                Some(q) => format!("No device matching \"{q}\" found."),
                None => "No devices found.".to_string(),
            };
            let others = node.registry.list();
            let also = if query.is_some() && !others.is_empty() {
                format!(
                    "\nDevices I can see: {}",
                    others
                        .iter()
                        .map(|p| p.info.alias.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                String::new()
            };
            bail!("{what}{also}\n\n{}", troubleshooting_tips())
        }
        1 if query.is_some() => Ok(peers.into_iter().next().expect("one")),
        _ => {
            let items: Vec<String> = peers.iter().map(describe).collect();
            let pick = tokio::task::spawn_blocking(move || {
                dialoguer::Select::new()
                    .with_prompt("Send to")
                    .items(&items)
                    .default(0)
                    .interact_opt()
            })
            .await??;
            match pick {
                Some(i) => Ok(peers[i].clone()),
                None => bail!("cancelled"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_addr;

    #[test]
    fn address_parsing() {
        assert_eq!(
            parse_addr("192.168.1.5").unwrap().to_string(),
            "192.168.1.5:47470"
        );
        assert_eq!(
            parse_addr("192.168.1.5:9000").unwrap().to_string(),
            "192.168.1.5:9000"
        );
        assert_eq!(parse_addr("[fe80::1]:9000").unwrap().port(), 9000);
        assert!(parse_addr("Riya's MacBook").is_none());
        assert!(parse_addr("riya.local").is_none());
    }
}
