# Simple RPC (Rust)

A minimal, working RPC server & client in Rust for the CS151 Disaggregated Systems assignment.

## Features
- TCP length‑prefixed JSON protocol (function name, params, request_id, error handling)
- Server implements:
  - `hash_compute` (SHA‑256 → 64‑char lowercase hex)
  - `sort_array` (ascending `i32` sort)
  - `matrix_multiply` (square `f64` row‑major, size n×n)
  - `compress_data` (zlib or lz4; returns base64‑encoded compressed bytes)
- Client exposes ergonomic async methods for each operation
- Uses `tokio`, `serde`, `sha2`, `flate2`, and `lz4_flex`

## Build & Run

```bash
cd simple-rpc-rust
# Run server
RPC_ADDR=0.0.0.0:8080 cargo run --bin server

# In another terminal, run client (defaults to 127.0.0.1:8080)
cargo run --bin client
```

Set `RPC_ADDR` env var on client to point elsewhere if the server runs remotely.

## Protocol

Each message is a 4‑byte big‑endian length prefix followed by a JSON object.

### Request
```json
{
  "request_id": "uuid-string",
  "func": "hash_compute",
  "params": { "data_base64": "..." }
}
```

### Response (success)
```json
{
  "status": "completed",
  "request_id": "uuid-string",
  "ok": true,
  "result": { "hex": "..." }
}
```

### Response (error)
```json
{
  "status": "error",
  "request_id": "uuid-string",
  "ok": false,
  "error": "error message"
}
```

## Notes
- Matrix multiply is executed on a blocking thread to avoid stalling the async runtime.
- Binary compression results are base64‑encoded in JSON responses.
- This is the **core** working implementation (no load generator yet).
