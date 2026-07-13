# Argus ML anomaly scorer

An **optional** unsupervised layer that augments — never replaces — the Go
rule-based detectors.

- Subscribes to order-flow feature vectors on NATS `features.*` (published by the
  Go detection engine).
- Fits an **Isolation Forest** online (warmup, then periodic refit on a rolling
  window) over 8 order-flow features (spread, order-flow imbalance, microprice
  deviation, trade intensity, signed volume, cancel ratio, book depth).
- Publishes advisory anomaly alerts on `alerts.ml`, which the dashboard shows
  alongside rule-based alerts.

## Why it's deliberately limited

| Choice | Reason |
|---|---|
| Advisory severity (medium/low) | Statistical outliers ≠ provable manipulation |
| **Not** written to the audit chain | The tamper-evident record holds only the *reproducible* rule decisions; a model whose output depends on its training window is kept out of the legal record of record |
| Cold-start warmup | The model scores nothing until it has seen enough normal flow |

This is how a real surveillance desk uses ML: triage/ranking on top of
deterministic rules, not as the system of record.

## Run

```bash
pip install -r requirements.txt
NATS_URL=nats://127.0.0.1:4222 python scorer.py
```

Tunables (env): `ML_WARMUP`, `ML_REFIT_EVERY`, `ML_BUFFER`, `ML_CONTAMINATION`,
`ML_COOLDOWN_S`.

## Test (offline, no NATS)

```bash
python test_scorer.py
```
