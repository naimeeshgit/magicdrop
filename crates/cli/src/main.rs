//! `magicdrop` — send files to nearby devices from the command line.

mod net;
mod receive;
mod send;

use anyhow::Result;
use clap::{Parser, Subcommand};
use magicdrop_core::Identity;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "magicdrop",
    version,
    about = "Send files to nearby devices on the same Wi-Fi.",
    after_help = "Examples:\n  magicdrop receive              wait for files\n  magicdrop send photo.jpg        pick a nearby device and send\n  magicdrop send Trip/ --to Riya  send a folder to Riya's device\n  magicdrop send a.pdf --to 192.168.1.20\n  magicdrop devices               list devices nearby"
)]
struct Cli {
    /// Show detailed logs.
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Folder for MagicDrop's settings and identity (default: OS config dir).
    #[arg(long, global = true, env = "MAGICDROP_HOME", hide_short_help = true)]
    home: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Wait for files from other devices.
    #[command(visible_alias = "r")]
    Receive {
        /// Where to save files (default: Downloads/MagicDrop).
        #[arg(short, long)]
        dir: Option<PathBuf>,
        /// Accept every transfer without asking. Only use on networks you trust.
        #[arg(short, long)]
        yes: bool,
        /// TCP port to listen on.
        #[arg(long, default_value_t = magicdrop_core::protocol::DEFAULT_PORT)]
        port: u16,
    },
    /// Send files or folders to a nearby device.
    #[command(visible_alias = "s")]
    Send {
        /// Files and/or folders to send.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Device name, id prefix, or IP address (IP:PORT also works).
        #[arg(short, long)]
        to: Option<String>,
        /// Seconds to look for devices.
        #[arg(short, long, default_value_t = 4)]
        wait: u64,
    },
    /// List devices nearby.
    #[command(visible_alias = "ls")]
    Devices {
        /// Seconds to look for devices.
        #[arg(short, long, default_value_t = 4)]
        wait: u64,
    },
    /// Show or change this device's name.
    Name {
        /// New name. Omit to show the current one.
        new_name: Option<String>,
    },
    /// Check network setup and explain problems.
    Doctor,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let filter = if cli.verbose {
        "magicdrop=debug,magicdrop_core=debug,info"
    } else {
        "magicdrop=warn,magicdrop_core=warn,warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    if let Err(e) = run(cli).await {
        eprintln!("\n✗ {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let home = cli.home.unwrap_or_else(Identity::default_home);
    let mut identity = Identity::load_or_create(&home)?;

    match cli.cmd {
        Cmd::Receive { dir, yes, port } => receive::run(&identity, dir, yes, port).await,
        Cmd::Send { paths, to, wait } => send::run(&identity, paths, to, wait).await,
        Cmd::Devices { wait } => send::list_devices(&identity, wait).await,
        Cmd::Name { new_name } => {
            if let Some(n) = new_name {
                identity.set_alias(&n)?;
                println!("This device is now called \"{}\".", identity.alias);
            } else {
                println!("{}", identity.alias);
                println!("id {}", &identity.id[..16]);
            }
            Ok(())
        }
        Cmd::Doctor => net::doctor(&identity).await,
    }
}
