#!/usr/bin/env python3
"""Argus ML anomaly scorer — an *optional* unsupervised layer that augments the
rule-based detectors.

It subscribes to the order-flow feature vectors the Go detection engine publishes
on NATS (``features.*``), fits an Isolation Forest online, and republishes an
anomaly alert on ``alerts.ml`` when order flow looks unusual in a way the
hard-coded rules might miss.

Design decisions (and honest limitations):

* **Augments, never replaces.** Isolation Forest flags statistical outliers, not
  provable manipulation. Its alerts are advisory and marked medium/low severity.
* **Not in the tamper-evident audit trail by design.** The audit chain records
  the *reproducible* rule-based decisions. A model whose output depends on its
  training window is deliberately kept out of the legal record of record and
  surfaced only on the dashboard.
* **Cold start.** The model needs a warmup window before it scores anything.

This mirrors how a real surveillance desk uses ML: as a triage/ranking aid on top
of deterministic rules, not as the system of record.
"""

import asyncio
import json
import os
import signal
import time
from collections import deque

import numpy as np
from sklearn.ensemble import IsolationForest
# `nats` is imported lazily in main() so the scoring logic can be unit-tested
# without the nats-py dependency installed.

# Canonical feature order — must match internal/features/features.go.
FEATURE_KEYS = [
    "spread_bps",
    "ofi",
    "microprice_dev_bps",
    "trade_intensity",
    "signed_vol",
    "cancel_ratio",
    "bid_depth",
    "ask_depth",
]

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
WARMUP = int(os.environ.get("ML_WARMUP", "150"))          # samples before first fit
REFIT_EVERY = int(os.environ.get("ML_REFIT_EVERY", "100"))  # refit cadence
BUFFER_MAX = int(os.environ.get("ML_BUFFER", "1000"))       # rolling training window
CONTAMINATION = float(os.environ.get("ML_CONTAMINATION", "0.04"))
COOLDOWN_S = float(os.environ.get("ML_COOLDOWN_S", "2.0"))  # per-symbol alert cooldown


def vectorize(features: dict) -> np.ndarray:
    return np.array([float(features.get(k, 0.0)) for k in FEATURE_KEYS], dtype=float)


class Scorer:
    def __init__(self, nc):
        self.nc = nc
        self.buffer = deque(maxlen=BUFFER_MAX)
        self.model = None
        self.since_fit = 0
        self.seq = 0
        self.last_alert = {}  # symbol -> monotonic ts

    def observe(self, vec: np.ndarray):
        self.buffer.append(vec)
        self.since_fit += 1
        if self.model is None and len(self.buffer) >= WARMUP:
            self._fit()
        elif self.model is not None and self.since_fit >= REFIT_EVERY:
            self._fit()

    def _fit(self):
        data = np.array(self.buffer)
        self.model = IsolationForest(
            n_estimators=100, contamination=CONTAMINATION, random_state=42
        ).fit(data)
        self.since_fit = 0

    def score(self, vec: np.ndarray):
        """Return (is_anomaly, normalized_score in [0,1]) or (None, None) if cold."""
        if self.model is None:
            return None, None
        # decision_function: negative => more anomalous.
        df = float(self.model.decision_function(vec.reshape(1, -1))[0])
        is_anom = df < 0
        # Map roughly [-0.3, 0.3] to a 0..1 "anomaly intensity".
        intensity = max(0.0, min(1.0, (0.15 - df) / 0.45))
        return is_anom, intensity

    async def handle(self, msg):
        try:
            payload = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        symbol = payload.get("symbol", "")
        feats = payload.get("features", {})
        vec = vectorize(feats)

        is_anom, intensity = self.score(vec)
        self.observe(vec)
        if not is_anom:
            return

        now = time.monotonic()
        if now - self.last_alert.get(symbol, 0.0) < COOLDOWN_S:
            return
        self.last_alert[symbol] = now

        await self._publish_alert(symbol, intensity, feats)

    async def _publish_alert(self, symbol, intensity, feats):
        self.seq += 1
        severity, label = (3, "high") if intensity > 0.66 else (2, "medium")
        alert = {
            "id": f"{symbol}-ml_anomaly-{self.seq}",
            "ts_ns": time.time_ns(),
            "detector": "ml_anomaly",
            "symbol": symbol,
            "severity": severity,
            "severity_label": label,
            "score": round(intensity, 4),
            "description": "unsupervised order-flow anomaly (ML, augments rule-based detectors)",
            "detect_latency_us": 0,
            "evidence": {
                "model": "isolation_forest",
                "intensity": round(intensity, 4),
                **{k: round(float(feats.get(k, 0.0)), 4) for k in FEATURE_KEYS},
                "note": "advisory only; not part of the tamper-evident audit trail",
            },
        }
        await self.nc.publish("alerts.ml", json.dumps(alert).encode())


async def main():
    import nats

    print(f"[argus-ml] connecting to {NATS_URL} …", flush=True)
    nc = await nats.connect(
        NATS_URL, reconnect_time_wait=1, max_reconnect_attempts=-1, name="argus-ml"
    )
    print("[argus-ml] connected; subscribing to features.*", flush=True)
    scorer = Scorer(nc)
    await nc.subscribe("features.*", cb=scorer.handle)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass
    await stop.wait()
    await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
