use crate::net::{reachable_addresses, start_node};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use magicdrop_core::server::{self, IncomingRequest, ReceiveEvent, ServerConfig};
use magicdrop_core::util::human_bytes;
use magicdrop_core::Identity;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

pub async fn run(identity: &Identity, dir: Option<PathBuf>, yes: bool, port: u16) -> Result<()> {
    let dir = dir.unwrap_or_else(server::default_download_dir);
    let mut cfg = ServerConfig::new(&dir);
    cfg.port = port;
    let mut node = start_node(identity, cfg).await?;

    println!("Ready to receive as \"{}\".", identity.alias);
    println!("Files go to {}", dir.display());
    let addrs = reachable_addresses(node.server.port);
    if addrs.is_empty() {
        println!("⚠ No Wi-Fi/Ethernet connection found. Others won't be able to reach you.");
    } else {
        println!("Address: {}", addrs.join("  "));
    }
    if yes {
        println!("Auto-accepting all transfers (--yes).");
    }
    println!("Waiting… (Ctrl+C to stop)\n");

    let mut session = SessionUi::default();
    loop {
        let ev = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                node.server.cancel_active().await;
                println!("\nStopped.");
                return Ok(());
            }
            ev = node.events.recv() => match ev {
                Some(ev) => ev,
                None => return Ok(()),
            },
        };
        match ev {
            ReceiveEvent::Request(req) => {
                if decide(req, yes).await {
                    // Progress bar is created lazily by the first file event.
                } else {
                    println!("Declined.\n");
                }
            }
            ReceiveEvent::FileStarted { file_id, size, .. } => {
                session.sizes.insert(file_id, size);
                session.bar();
            }
            ReceiveEvent::FileProgress {
                file_id, received, ..
            } => {
                session.progress.insert(file_id, received);
                session.update();
            }
            ReceiveEvent::FileSaved { saved_to, .. } => {
                session.saved.push(saved_to);
            }
            ReceiveEvent::FileFailed { error, .. } => {
                session.println(&format!("  ✗ a file failed: {error}"));
            }
            ReceiveEvent::SessionFinished { saved, failed, .. } => {
                session.finish();
                if failed == 0 {
                    println!(
                        "✓ Received {saved} file{} in {}\n",
                        plural(saved),
                        dir.display()
                    );
                } else {
                    println!(
                        "⚠ Received {saved} file{}, {failed} failed. Saved files are in {}\n",
                        plural(saved),
                        dir.display()
                    );
                }
                session = SessionUi::default();
            }
            ReceiveEvent::SessionCancelled { reason, .. } => {
                session.finish();
                println!("✗ Transfer stopped: {reason}\n");
                session = SessionUi::default();
            }
        }
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

/// Show the request and ask the user. Returns whether it was accepted.
async fn decide(req: IncomingRequest, yes: bool) -> bool {
    let n = req.files.len();
    println!(
        "📨 {} wants to send {} file{} ({}):",
        req.sender.alias,
        n,
        plural(n),
        human_bytes(req.total_bytes)
    );
    for f in req.files.iter().take(8) {
        println!("   {}  {}", f.path, human_bytes(f.size));
    }
    if n > 8 {
        println!("   … and {} more", n - 8);
    }
    if yes {
        req.accept();
        return true;
    }

    // dialoguer blocks, so run it off the async runtime. If the sender gives up
    // while we're asking, the answer is simply ignored.
    let question = format!("Accept from {}?", req.sender.alias);
    let answer = tokio::task::spawn_blocking(move || {
        dialoguer::Confirm::new()
            .with_prompt(question)
            .default(false)
            .interact()
            .unwrap_or(false)
    });
    let accepted = matches!(
        tokio::time::timeout(
            Duration::from_secs(magicdrop_core::protocol::CONSENT_TIMEOUT_SECS),
            answer
        )
        .await,
        Ok(Ok(true))
    );
    if req.is_expired() {
        println!("(The sender stopped waiting.)\n");
        return false;
    }
    if accepted {
        req.accept();
    } else {
        req.decline();
    }
    accepted
}

#[derive(Default)]
struct SessionUi {
    sizes: HashMap<String, u64>,
    progress: HashMap<String, u64>,
    saved: Vec<PathBuf>,
    pb: Option<ProgressBar>,
}

impl SessionUi {
    fn bar(&mut self) {
        let total: u64 = self.sizes.values().sum();
        match &self.pb {
            Some(pb) => pb.set_length(total.max(pb.length().unwrap_or(0))),
            None => {
                let pb = ProgressBar::new(total);
                pb.set_style(
                    ProgressStyle::with_template(
                        "  {bar:30.cyan/blue} {bytes}/{total_bytes}  {binary_bytes_per_sec}  {eta}",
                    )
                    .expect("valid template")
                    .progress_chars("█▉▊▋▌▍▎▏ "),
                );
                self.pb = Some(pb);
            }
        }
    }

    fn update(&mut self) {
        if let Some(pb) = &self.pb {
            pb.set_position(self.progress.values().sum());
        }
    }

    fn println(&self, s: &str) {
        match &self.pb {
            Some(pb) => pb.println(s),
            None => println!("{s}"),
        }
    }

    fn finish(&mut self) {
        if let Some(pb) = self.pb.take() {
            pb.finish_and_clear();
        }
    }
}
