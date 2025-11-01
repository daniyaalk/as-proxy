## as-proxy

A lightweight, multi-port TCP proxy with Aerospike protocol awareness. It can run as a plain proxy or intercept Aerospike read/write traffic to:

- Respond to write operations immediately (without forwarding) and cache the resulting mutations for a short TTL
- Respond to subsequent read operations with synthetic data based on the cached mutations
- Optionally integrate with Kafka (feature-gated) to produce or consume replay records

Built with Tokio for async IO and `tracing` for structured logging.

### Features

- Multi-port proxying via a simple TOML config
- Optional write interception and synthetic read responses
- TTL-backed in-memory diff cache
- Kafka replay integration (feature `replay`)
- Hex/ASCII packet tracing for debugging

### Status and scope

- Aerospike protocol support: info (0x01) and message (0x03)
- Only a safe subset of flags is supported when interception is enabled; unsupported flags will cause the process to terminate to prevent unsafe behavior
- Intended for development, QA, and replay workflows—not a production data plane

---

## Quick start

### Prerequisites

- Rust (edition 2024) – install via `https://rustup.rs`
- Kafka is optional, required only when using the `replay` feature

### Build

```bash
cargo build --release
# With Kafka replay feature
cargo build --release --features replay
```

### Run

```bash
# Uses config.toml by default
cargo run --release

# Or specify a custom config path
cargo run --release -- --config ./config.toml

# With Kafka replay feature
cargo run --release --features replay -- --config ./config.toml
```

Enable logs with an env filter (defaults to `debug` if unset):

```bash
RUST_LOG=info cargo run --release -- --config ./config.toml
```

---

## Configuration

`as-proxy` is configured via a TOML file (default: `config.toml`). Example:

```toml
# Intercept and short-circuit write requests; serve synthetic reads from cache
# intercept_writes = true

# TTL (seconds) for synthetic diffs cached per Aerospike key
diff_ttl = 120

[mappings]
"4000" = "127.0.0.1:3000"
"4001" = "tpap-cache-2:3000"
"4002" = "tpap-cache-3:3000"

# Only used when compiled with `--features replay`
[kafka_config]
hosts = "localhost:9092"
topic = "test"
mode  = "Consume" # or "Produce"
```

### Fields

- mappings: map of listen-port (string) to upstream `host:port`
- intercept_writes (bool, optional):
  - When true, write ops are not forwarded; a successful write response is returned immediately and the operation set is cached for `diff_ttl` seconds
  - Reads to the same key are answered from the cached operations if present
- diff_ttl (u64): TTL in seconds for cached diffs
- kafka_config (feature `replay` only):
  - hosts (string): comma-separated Kafka brokers (e.g., `"localhost:9092,localhost:9093"`)
  - topic (string): topic to produce/consume replay records
  - mode (enum): `Produce` or `Consume`

---

## How it works

At startup, `as-proxy` binds listeners for each configured `mappings` entry. For each inbound connection:

- In plain proxy mode, bytes are relayed bidirectionally
- When interception is active (either `intercept_writes = true` or Kafka consumer mode):
  - Client→Server: Aerospike messages are parsed. Write ops are short-circuited and cached; reads may be answered from the cache
  - Server→Client: In `replay`+`Produce` mode, responses are serialized and produced to Kafka alongside the key

Unsupported Aerospike flags are rejected early to avoid undefined behavior.

---

## Kafka replay (optional)

This capability is behind the `replay` feature flag.

- Produce mode: publishes `ReplayRecord` JSON messages to `kafka_config.topic`
- Consume mode: consumes `ReplayRecord` JSON messages and populates the in-memory diff cache

Build and run with:

```bash
cargo run --release --features replay -- --config ./config.toml
```

Record schema (conceptual):

```json
{
  "key": {
    "namespace": "<string>",
    "set": "<string>",
    "digest": "<bytes-as-array>"
  },
  "operations": [
    {
      "op": 0,
      "particle_type": 0,
      "bin_version": 0,
      "bin_name": "<string>",
      "data": "<bytes-as-array>"
    }
  ]
}
```

---

## CLI

```text
as-proxy --config <PATH>

Options:
  -c, --config <PATH>  Path to config TOML [default: config.toml]
  -h, --help           Print help
  -V, --version        Print version
```

---

## Logging

`tracing_subscriber` is used for logs. Control verbosity with `RUST_LOG` (fallback is `debug`). Examples:

```bash
RUST_LOG=info as-proxy
RUST_LOG=as_proxy=debug,hyper=warn,rdkafka=warn as-proxy
```

---

## Development

- Format and lint with standard Rust tooling
- Tests are not yet included; exercise using integration scenarios against a test Aerospike service

### Run locally against multiple upstreams

1. Define ports→upstreams in `[mappings]`
2. Start `as-proxy`
3. Point clients to the local ports (e.g., `localhost:4000`)

---

## Known limitations

- Only Aerospike info (0x01) and message (0x03) frames are parsed
- Interception currently focuses on simple read/write flows
- Strict feature mask validation; unsupported flags will terminate the process

---

## License

Add a license file if distributing publicly (e.g., MIT/Apache-2.0).


