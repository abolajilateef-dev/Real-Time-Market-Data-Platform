# Market Data Platform

A real-time market data platform in Go that ingests L2 order book updates from multiple cryptocurrency exchanges, reconstructs full order books with sequence-gap detection and snapshot recovery, and fans normalized data out to downstream consumers over Kafka.

## What it does

Trading firms need a clean, correct, and low-latency view of the market. Raw exchange feeds are noisy: every venue speaks a different protocol, messages drop, sequence numbers skip, and out-of-the-box you get thousands of standing offers changing hundreds of times per second.

This platform is the layer that turns exchange chaos into consumable truth:

- Ingests trades and L2 order book updates from Coinbase and Binance over WebSocket
- Normalizes every venue's format into a shared internal schema
- Publishes to Kafka (Redpanda) for durable, replayable fan-out
- Maintains in-memory order books with sequence-gap detection and snapshot recovery
- Persists full history to TimescaleDB for backtesting and replay
- Computes real-time analytics (spread, mid, order-book imbalance, VWAP)
- Exposes a WebSocket API for external consumers
- Instruments everything with Prometheus and Grafana

## Architecture

<!-- TODO: add architecture diagram here (mermaid or SVG) -->

Exchange WebSocket feeds → Feed Handlers → Kafka → Book Service → BookTop / Analytics / Persister → downstream consumers.

**Components:**

| Service | Job |
|---|---|
| `feedhandler-coinbase` | Connect to Coinbase WS, normalize, publish to Kafka |
| `feedhandler-binance` | Same, for Binance |
| `bookservice` | Consume book updates, maintain L2 books, publish top-of-book |
| `persister` | Write raw deltas and trades to TimescaleDB |
| `wspublisher` | Fan out to external WebSocket clients |

## Design decisions

<!-- This section is where you show interviewers you *think*. Filled in as we build. -->

- **Absolute-size deltas, not additive.** Every delta overwrites the level's size, matching how exchanges actually publish.
- **Kafka keyed by symbol.** Preserves per-symbol ordering while allowing parallel consumption across symbols.
- **Per-venue book maintenance.** One book per `(venue, symbol)` pair; consolidation is a downstream concern, not built in.
- **JSON serialization to start, Protobuf planned.** Human-readable for debugging, then migrated for size/latency wins (with before/after measurements).
- **`float64` for prices.** Documented tradeoff — production would use fixed-point decimal to avoid rounding errors that compound across fills.

## Tech stack

- **Language:** Go 1.23
- **Message bus:** Redpanda (Kafka-compatible)
- **Storage:** TimescaleDB (PostgreSQL time-series extension)
- **Observability:** Prometheus + Grafana
- **Kafka client:** [franz-go](https://github.com/twmb/franz-go)
- **Deployment:** Docker Compose

## Running it

<!-- TODO: fill in once we have working services -->

```bash
# Bring up infrastructure
make up

# Run each service (separate terminals)
make run-feed-coinbase
make run-bookservice
make run-persister
```

Then open:
- Redpanda Console: http://localhost:8080
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090

## Project structure
```
mdp/
├── cmd/ # Runnable services (one subfolder per binary)
├── pkg/schema/ # Shared data contracts (Trade, BookUpdate, BookTop, ...)
├── internal/ # Private helpers (Kafka, observability, config)
├── deploy/ # Docker Compose, Prometheus, Grafana config
├── configs/ # Runtime config (symbol mappings, etc.)
└── go.mod
```

## Measured performance

<!-- TODO: fill in with real numbers after Phase 2 -->

- Ingest-to-book latency (p50 / p99): _pending_
- Throughput sustained: _pending_ messages/sec
- Gap recovery time (median): _pending_ ms

## What's next

<!-- TODO: fill in as we go -->

- Second venue (Binance)
- Protobuf migration
- Consolidated best bid and offer across venues
- Chaos testing: simulated feed disconnects, sequence gap injection

---

Built as a portfolio project. Feedback and issues welcome.
