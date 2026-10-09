# MagicDrop protocol v1

Everything happens on the local network. No servers, no accounts.

## Identity

Each install creates a self-signed certificate on first run. The device **id**
is the lowercase hex SHA-256 of the certificate's DER bytes (64 chars). The
**alias** is a display name the user picks (≤ 48 chars).

> v1 uses plain HTTP. Phase 3 switches the same endpoints to mutual TLS using
> this certificate, with peers pinning the id.

## Discovery

Two mechanisms run in parallel; either is enough.

### mDNS / DNS-SD

- Service type `_magicdrop._tcp.local.`, instance `magicdrop-<first 12 hex of id>`.
- SRV port = HTTP API port.
- TXT: `id`, `alias`, `type` (`laptop`/`desktop`/`phone`/…), `os`, `ver`, `proto` (= `1`).

### UDP multicast

- Group `224.0.0.199`, UDP port `47471`, TTL 1. Sent on every LAN interface.
- Payload (JSON):

```json
{"magicdrop":1,"kind":"announce","info":{"id":"…","alias":"Riya's MacBook","device_type":"laptop","os":"macos","version":"0.1.0","protocol":1,"port":47470}}
```

- On receiving an `announce` from someone else, a device:
  1. unicasts a `reply` (same shape, `"kind":"reply"`) to the sender's IP, port 47471;
  2. calls the sender's `POST /register` with its own info.
- `reply` messages are never answered (no storms).
- Devices announce at startup (×3, to survive Wi-Fi packet loss) and every 20 s.

Peers are deduplicated by `id` and expire after 120 s of silence.

## HTTP API

Base: `http://<ip>:<port>/api/magicdrop/v1`. Default port `47470` (random if
taken; the real port is always the one advertised). JSON bodies. Errors return
`{"error": "…"}`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/info` | Device info (`DeviceInfo`) |
| POST | `/register` | Body: caller's `DeviceInfo`. Returns ours. |
| POST | `/prepare` | Ask to send files. Blocks until the user decides (≤ 60 s). |
| PUT | `/upload?session&file&token` | Raw file bytes |
| POST | `/finalize?session&file&token` | Body `{"sha256": "<hex>"}`; commit the file |
| POST | `/complete?session` | Sender is done |
| POST | `/cancel?session` | Sender aborts |

### Transfer flow

1. **Prepare.** `{"sender": DeviceInfo, "files": [{"id":"f0","path":"Trip/a.jpg","size":123}]}`
   - File ids: `[A-Za-z0-9_-]{1,64}`, unique per request.
   - Paths are relative, `/`-separated. The receiver rejects `..`, absolute
     paths, drive letters, `:`, and control characters with **400** before the
     user is ever asked.
   - **200** `{"session_id": "…", "tokens": {"f0": "…"}}` — accepted.
   - **403** declined or no answer in 60 s. **409** busy. **503** not receiving.
2. **Upload** each file (up to 3 in parallel): `PUT` with exactly `size` bytes.
   The receiver writes to a hidden `.<name>.<id>.mdpart` file next to the final
   location and returns `{"file_id","bytes","sha256"}`.
   - Requests must come from the same IP that called `/prepare`.
   - **413** if more than `size` bytes arrive; **400** if fewer.
3. **Finalize** with the sha256 the *sender* computed while reading the file.
   - Match → the part file is renamed into place. If the name exists, it
     becomes `name (1).ext`, `name (2).ext`, … Never overwrites.
   - Mismatch → **422**, part deleted. The sender may retry the upload.
4. **Complete.** The receiver ends the session. Sessions also end automatically
   when all files are saved, on `/cancel`, or after 120 s of inactivity (part
   files are deleted).

One session at a time per receiver.
