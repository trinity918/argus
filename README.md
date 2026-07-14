# Argus — Real-Time Market Surveillance Engine

![Go](https://img.shields.io/badge/Go-1.25-00ADD8) ![Python](https://img.shields.io/badge/Python-3.12-3776AB) ![License](https://img.shields.io/badge/license-MIT-green)

A streaming system that ingests live order-book and trade feeds from **multiple
exchanges** (Binance, OKX), detects market-manipulation **footprints** (spoofing,
layering, momentum ignition, quote stuffing, wash trading) in real time, and
writes every alert to a **tamper-evident, hash-chained, Ed25519-signed audit
trail**. Fully distributed over NATS, one-command deployable to **Azure
Container Apps**.

This is the kind of system an exchange's market-integrity / compliance-engineering
team runs to guard market integrity: raw feed in → actionable, low-latency,
provably-unaltered alerts out.

> **Honest framing up front.** Public exchange feeds are *anonymous* — there are no
> participant IDs. So Argus detects the market *footprints consistent with*
> manipulation on the L2 tape, which is exactly the signal a surveillance desk
> triages before pulling attributed order data. It does not, and cannot from L2
> alone, prove intent. Every design choice below reflects that honesty.

---

## Why this design

| Requirement | How Argus meets it |
|---|---|
| Turn data into action | Raw market feed → normalized events → actionable alerts |
| Low-latency infrastructure | In-memory streaming detectors; **~543 ns/event (~1.8M events/sec/core, 1 alloc/op)** on the hot path; end-to-end detection latency in single-digit µs |
| Massively scalable | Per-venue ingestors + **symbol-sharded detectors** (`-subjects md.BTC*`) scale independently over NATS with zero coordination |
| Multi-venue | Two live adapters (Binance U/u sequencing, OKX seqId+CRC32 checksums) normalize into one schema — detectors never change per venue |
| Proactively guard / risk | Market-integrity surveillance across five manipulation patterns |
| Machine learning | Optional Isolation Forest anomaly layer that *augments* (never replaces) the rules |
| Auditability / non-repudiation | Hash-chained + Merkle-checkpointed + signed audit log with an independent verifier |
| Cloud-deployable | One command → Azure Container Apps via Bicep IaC ([deploy/azure](deploy/azure)) |

The audit trail is the differentiator: surveillance projects almost never have a
cryptographic non-repudiation story, and provable "who-flagged-what-when, unaltered"
is precisely what a compliance system needs.

---

## Architecture

```
   Binance WS/REST ───────┌───────────────┐
   OKX WS (books+trades) ─▶│  Ingestors    │  normalize • book sync (U/u | seqId+CRC32) • backpressure
   (or synthetic replay)  │  (Go, 1/venue)│  reconnect • sequence-gap resync
                          └──────┬────────┘
                                 │  md.<symbol>                    NATS subjects (or in-proc bus)
                                 ▼
                          ┌───────────────┐   features.<symbol>   ┌──────────────┐
                          │   Detector    │ ─────────────────────▶│  ML scorer   │
                          │     (Go)      │ ◀────── alerts.ml ─────│  (Python)    │
                          │ spoofing •    │                        │ IsolationF.  │
                          │ layering •    │                        └──────────────┘
                          │ momentum •    │
                          │ stuffing•wash │  alerts.rule
                          └──────┬────────┘
                                 │  append BEFORE publish
                                 ▼
                    ┌──────────────────────────┐
                    │   Tamper-evident audit    │  SHA-256 hash chain
                    │  audit.log + checkpoints  │  + Merkle checkpoints
                    └──────────┬───────────────┘   + Ed25519 signatures
                                 │  alerts.>  •  audit.checkpoint
                                 ▼
                          ┌───────────────┐
                          │   API + WS    │  dashboard • live feed • /api/audit/verify • /metrics
                          │     (Go)      │
                          └───────────────┘
```

The transport is a small `Bus` interface with two implementations: an **in-process**
bus (single binary, zero dependencies) and **NATS** (independently-scaled services).
The exact same detectors run in both.

---

## Quickstart

### Option A — single binary, no dependencies

```bash
# Synthetic manipulation tape (deterministic; triggers all detectors)
go run ./cmd/argusd
# → dashboard at http://localhost:8080

# Or against the live Binance feed (needs internet)
go run ./cmd/argusd -live -symbols BTCUSDT,ETHUSDT
```

### Option B — full distributed stack over NATS (Docker)

```bash
# Self-contained demo (synthetic tape, no external data)
docker compose --profile demo up --build

# Live Binance feed
docker compose --profile live up --build
```

Open **http://localhost:8080** for the dashboard: a live alert feed, throughput
tiles, and a one-click **Verify Chain** button that cryptographically re-verifies
the entire audit trail.

### Verify the audit trail from the CLI

```bash
go run ./cmd/auditverify -dir ./data/audit
# ✓ VERIFIED — chain intact, checkpoints valid   (exit 0)
# tamper with any byte of data/audit/audit.log and it exits non-zero,
# naming the exact sequence number that was altered.
```

---

## Detectors

All detectors are sliding-window state machines over the L2 tape. Thresholds live
in [`internal/detect/config.go`](internal/detect/config.go) and are individually
documented (each is a sensitivity/false-positive tradeoff).

| Detector | Footprint it flags | Severity | Notes |
|---|---|---|---|
| **spoofing** | Large order appears near touch, then is cancelled unexecuted before the market reaches it | high | "Large" is vs a self-calibrating EMA of recent resting size |
| **layering** | The spoof footprint stacked across ≥3 same-side levels, pulled in concert | critical | |
| **momentum_ignition** | Aggressive one-sided burst moves price ≥N bps, then rapidly reverses | high | Two-phase leg→reversal state machine |
| **quote_stuffing** | Extreme book-update rate with negligible executions | medium | Churn without price discovery |
| **wash_trade** | Offsetting prints clustered in a razor-thin band with no price discovery | low | **Low by construction** — anonymous data can't confirm common ownership |
| **ml_anomaly** | Statistical outlier in order-flow features | medium | Optional; advisory; *not* in the audit chain |

**Alert throttling.** Spoofing/layering emission is cooldown-throttled
(`SpoofCooldown`, `LayeringCooldown`): on a volatile live tape, qualifying
add-then-pull sequences occur many times per second, and an unthrottled feed is
a storm nobody can triage (measured: ~1,190 alerts/min unthrottled vs ~1/min
throttled on the same live dual-venue feed). Pattern *tracking* stays
unthrottled — layering still accumulates every pull — only emission is limited.

### Honest limitations

- **Anonymity.** No participant IDs on public feeds, so these are footprints, not
  attributed intent. Wash-trade detection is explicitly low-confidence and every
  such alert says so in its evidence.
- **L2 aggregation.** Depth diffs give net size changes per level, not individual
  orders; spoofing is inferred from add-then-pull size dynamics.
- **Tail truncation.** The hash chain detects mutation, reordering, and insertion,
  and signed checkpoints anchor everything up to the last checkpoint. Wholesale
  truncation *after* the last checkpoint is the one thing a self-contained log
  can't prove; the mitigation (external anchoring of checkpoints) is noted in the
  audit package docs.

---

## The tamper-evident audit trail

Every rule-based alert is appended to an append-only log as a hash-chain entry:

```
hash_n = SHA-256( domain ‖ seq ‖ ts ‖ len(payload) ‖ payload ‖ hash_{n-1} )
```

Periodically the chain is **checkpointed**: a Merkle root over the entries in the
range (RFC-6962-style domain separation) is computed and **Ed25519-signed**. The
independent verifier ([`cmd/auditverify`](cmd/auditverify) / `/api/audit/verify`)
re-walks the chain, recomputes every hash, rebuilds every Merkle root, and checks
every signature against a **pinned** public key. It precisely locates any:

- **mutation** — payload change → hash mismatch at that sequence
- **reordering / insertion** — sequence or chain-link break
- **truncation below a checkpoint** — checkpoint references a missing entry

This is unit-tested for each failure mode in
[`internal/audit/audit_test.go`](internal/audit/audit_test.go).

Design choice: the ML layer's alerts are **deliberately excluded** from the audit
chain. The tamper-evident record holds only the *reproducible* rule-based
decisions; a model whose output depends on its training window has no place in the
legal record of record.

---

## Latency & observability

Every service exposes Prometheus metrics on `/metrics`:

| Metric | Meaning |
|---|---|
| `argus_detection_latency_microseconds` | Histogram: event ingest → alert emission |
| `argus_processing_latency_microseconds` | Histogram: per-event engine processing |
| `argus_alerts_total{detector,severity}` | Alerts by type |
| `argus_audit_entries_total` / `argus_audit_checkpoints_total` | Audit activity |
| `argus_events_processed_total` / `argus_features_emitted_total` | Throughput |
| `argus_ingest_*` | Frames received/dropped, reconnects, book resyncs, snapshots |

In the in-process demo, detection latency is typically **≤5 µs** per alert.

---

## Data flow (bus subjects)

| Subject | Producer | Consumer |
|---|---|---|
| `md.<symbol>` | ingestor / replay | detector |
| `features.<symbol>` | detector | ML scorer |
| `alerts.rule` | detector | API (`alerts.>`) |
| `alerts.ml` | ML scorer | API (`alerts.>`) |
| `audit.checkpoint` | detector | API |

The in-proc `Bus` reproduces NATS wildcard semantics (`*`, `>`) so behavior is
identical across topologies.

---

## Ingestion correctness

The ingestor implements Binance's documented "manage a local order book correctly"
algorithm as a **pure, exhaustively unit-tested state machine**
([`internal/exchange/binance/booksync.go`](internal/exchange/binance/booksync.go)):
buffer diffs → REST snapshot → drop `u ≤ lastUpdateId` → validate the first diff
brackets `lastUpdateId+1` → require `U == lastU+1` thereafter, resyncing on any gap.
It also tolerates load-shed drops: a detected gap triggers a resync that restores a
correct book rather than silently corrupting it. Prices and sizes use **exact
fixed-point int64** arithmetic (no float keys, no drift) so runs over the same tape
are byte-reproducible in the audit log.

Proven against the real exchange:

```bash
make test-live   # ARGUS_LIVE=1 go test ./internal/exchange/binance -run TestLiveBinanceSmoke -v
```

---

## Project layout

```
cmd/
  argusd/       all-in-one daemon (in-proc bus) — the zero-dependency demo
  ingestor/     Binance → NATS ingestion service
  detector/     NATS → detection + audit service
  api/          dashboard / WebSocket / verification service
  replay/       publish the synthetic scenario to NATS
  auditverify/  standalone audit-trail verifier (exit code = integrity)
internal/
  fixedpoint/   exact base-10 int64 decimal
  events/       normalized Trade/Depth schema
  orderbook/    L2 book with per-level deltas
  exchange/binance/  WS client + U/u book-sync state machine
  exchange/okx/      WS client + seqId continuity + CRC32 checksum ladder
  transport/    Bus interface: in-proc + NATS
  detect/       sliding-window detectors + engine + feature extraction
  audit/        hash chain + Merkle + Ed25519 + verifier
  features/     shared feature-vector schema (Go ⇄ Python)
  metrics/      Prometheus instrumentation
  api/          HTTP/WS server + embedded dashboard
  app/          service wiring (shared by argusd and the split services)
  scenario/     scripted manipulation tape (demo + integration test)
ml/             Python Isolation Forest scorer
deploy/azure/   Bicep IaC + deploy script for Azure Container Apps
```

---

## Testing

```bash
make test        # full Go suite (offline): fixedpoint, orderbook, book-sync,
                 # every detector, audit tamper/reorder/truncation, end-to-end
make test-race   # same, with the race detector
make test-live   # live Binance smoke test (needs internet)
make test-ml     # Python ML scorer offline test
```

The end-to-end test ([`internal/app/e2e_test.go`](internal/app/e2e_test.go)) drives
the full manipulation tape through the real engine and audit chain with a
deterministic virtual clock, asserts every detector fired, verifies the trail, then
tampers with it and asserts verification fails.

---

## Deploying to Azure

One command deploys the full distributed stack to **Azure Container Apps**:

```bash
az login
./deploy/azure/deploy.sh argus-rg eastus demo   # or: live
# → prints the public dashboard URL
```

[`deploy/azure/main.bicep`](deploy/azure/main.bicep) provisions everything as
code: Container Apps environment + Log Analytics, ACR (images built cloud-side
with `az acr build`, pulled via **managed identity** — no registry passwords),
an Azure Files share for the audit trail (detector writes, API verifies), NATS
on internal TCP ingress, KEDA autoscaling for the API, and the feed source
selected by `feedMode` (synthetic replay, or live Binance + OKX ingestors).
`maxReplicas: 1` on the detector is deliberate — the hash chain is
single-writer; scale out with additional detector apps on disjoint `-subjects`
filters. CD runs the same script from GitHub Actions via OIDC
([deploy-azure.yml](.github/workflows/deploy-azure.yml)) — no long-lived cloud
secrets. Full runbook: [deploy/azure/README.md](deploy/azure/README.md).

## Scaling model

- **Ingestion**: one ingestor per venue (`-exchange binance|okx`); add venues by
  adding adapters, add throughput by splitting symbol lists across instances.
- **Detection**: shard by subject filter — `detector -subjects "md.BTCUSDT"` and
  `detector -subjects "md.ETH*"` partition the symbol space with zero
  coordination, because all detector state is strictly per-symbol. Each shard
  writes its own audit chain (single-writer by design).
- **Measured single-core engine throughput** (`make bench`, i7-12650H):
  ~543 ns per depth event ≈ **1.8M events/sec/core** with 1 alloc/op; 35 µs per
  event on the worst-case mixed tape (bulk snapshots + 50-level stuffing bursts).

---

## Design decisions worth discussing

- **Exact fixed-point over float64** — reproducibility to the last digit; float keys
  in an order book are a latent bug.
- **Pure state machines for the hard parts** — book sequencing and detectors are
  I/O-free and unit-tested against crafted scenarios.
- **Backpressure by shedding, not blocking** — bounded queues drop under overload;
  correctness is restored by resync, keeping memory bounded (the surveillance
  equivalent of "fail safe").
- **Core NATS vs JetStream** — market data is fine at-most-once (drop stale, resync);
  the audit/alert path is where you'd add JetStream durability. The `Bus` seam makes
  that a config change, not a rewrite.
- **ML augments, never replaces** — and stays out of the legal record of record.

## License

MIT
