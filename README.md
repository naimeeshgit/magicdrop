# MagicDrop

AirDrop-style file sharing for Windows and Mac. Two laptops on the same Wi-Fi
find each other automatically, and files go straight from one to the other —
no internet, no cloud, no accounts.

> **Status: v0.1, command-line preview.** Discovery and transfers work. Next up:
> encryption (TLS), then the desktop app with drag-and-drop.

## Download

1. Open the [Actions tab](https://github.com/naimeeshgit/magicdrop/actions),
   click the latest green **CI** run, and download from **Artifacts**:
   - `magicdrop-windows-x64` — most Windows laptops
   - `magicdrop-windows-arm64` — Snapdragon / ARM Windows laptops
   - `magicdrop-macos-universal` — any Mac (Intel or Apple Silicon)
2. Unzip it.

Tagged releases also appear under [Releases](https://github.com/naimeeshgit/magicdrop/releases).

### First run on Windows

Open PowerShell in the unzipped folder:

```powershell
Unblock-File .\magicdrop.exe     # removes the "downloaded from internet" flag
.\magicdrop.exe receive
```

When Windows Firewall asks, tick **Private networks** and click **Allow**.
If the other device can't see you, set your Wi-Fi to *Private*:
Settings → Network & internet → Wi-Fi → (your network) → Network profile type → Private.

### First run on Mac

Open Terminal in the unzipped folder:

```bash
xattr -d com.apple.quarantine magicdrop   # allow the unsigned download
chmod +x magicdrop
./magicdrop receive
```

If macOS asks whether to accept incoming network connections, click **Allow**.

## Use

On the device **receiving** files:

```bash
magicdrop receive
```

On the device **sending**:

```bash
magicdrop send photo.jpg                 # shows nearby devices, pick one
magicdrop send Trip/ notes.pdf --to Riya # by name (partial names work)
magicdrop send movie.mp4 --to 192.168.1.20   # by IP if discovery is blocked
```

The receiver sees who's sending and what, and has to say yes. Files land in
`Downloads/MagicDrop`. Existing files are never overwritten.

Other commands:

```bash
magicdrop devices          # who's nearby?
magicdrop name "Time's Laptop"   # change how you appear to others
magicdrop doctor           # diagnose network problems
```

## When devices can't see each other

- **Campus / office / hotel Wi-Fi** usually blocks device-to-device traffic.
  Turn on a **phone hotspot** and connect both laptops to it.
- **Windows:** allow MagicDrop in the firewall prompt and set Wi-Fi to *Private*.
- **VPN:** pause it, or enable "allow LAN access".
- Discovery blocked but the network allows traffic? Use `--to <IP>`.
  `magicdrop receive` prints its address.

## How it works

- Discovery: mDNS (`_magicdrop._tcp`) **and** UDP multicast on
  `224.0.0.199:47471`, in parallel.
- Transfer: HTTP on port `47470` — prepare → user accepts → stream each file →
  verify SHA-256 → atomic rename into place.
- Details: [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Build from source

Requires [Rust](https://rustup.rs).

```bash
cargo test --all
cargo build --release      # binary at target/release/magicdrop
```

Run two instances on one machine for testing:

```bash
MAGICDROP_HOME=/tmp/a magicdrop receive --dir /tmp/inbox
MAGICDROP_HOME=/tmp/b magicdrop send somefile --to 127.0.0.1
```

## Roadmap

- [x] Phase 0–2: workspace, discovery, transfers, CLI
- [ ] Phase 3: mutual TLS, pinned device ids, verification code on first contact
- [ ] Phase 4: desktop app (Tauri) — device grid, drag-and-drop, tray, accept popup
- [ ] Phase 5: resume interrupted transfers, IPv6 / iPhone hotspot, firewall auto-fix
- [ ] Phase 6: Windows installer, "Send to MagicDrop" in Explorer
