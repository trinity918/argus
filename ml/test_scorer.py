"""Offline unit test for the ML scoring logic (no NATS required).

Run: python -m pytest ml/test_scorer.py   (or: python ml/test_scorer.py)
"""

import numpy as np

from scorer import Scorer, vectorize, FEATURE_KEYS


def test_vectorize_orders_and_defaults():
    v = vectorize({"spread_bps": 1.5, "ofi": -0.2})
    assert v.shape == (len(FEATURE_KEYS),)
    assert v[0] == 1.5 and v[1] == -0.2
    assert v[2] == 0.0  # missing key defaults to 0


def test_flags_obvious_anomaly_after_warmup():
    rng = np.random.default_rng(0)
    s = Scorer(nc=None)

    # Warm up on a tight "normal" order-flow cluster.
    for _ in range(200):
        feats = {
            "spread_bps": 1.0 + rng.normal(0, 0.05),
            "ofi": rng.normal(0, 0.05),
            "microprice_dev_bps": rng.normal(0, 0.1),
            "trade_intensity": 5 + rng.normal(0, 0.5),
            "signed_vol": rng.normal(0, 0.2),
            "cancel_ratio": 0.5 + rng.normal(0, 0.02),
            "bid_depth": 20 + rng.normal(0, 1),
            "ask_depth": 20 + rng.normal(0, 1),
        }
        s.observe(vectorize(feats))

    assert s.model is not None, "model should be fit after warmup"

    normal = vectorize({
        "spread_bps": 1.0, "ofi": 0.0, "microprice_dev_bps": 0.0,
        "trade_intensity": 5, "signed_vol": 0.0, "cancel_ratio": 0.5,
        "bid_depth": 20, "ask_depth": 20,
    })
    is_anom_normal, _ = s.score(normal)
    assert is_anom_normal is False

    # A wildly off-distribution vector (quote-stuffing-like churn) must flag.
    anomaly = vectorize({
        "spread_bps": 12.0, "ofi": 0.95, "microprice_dev_bps": 8.0,
        "trade_intensity": 0.1, "signed_vol": 0.0, "cancel_ratio": 0.99,
        "bid_depth": 300, "ask_depth": 2,
    })
    is_anom, intensity = s.score(anomaly)
    assert is_anom is True
    assert 0.0 <= intensity <= 1.0


if __name__ == "__main__":
    test_vectorize_orders_and_defaults()
    test_flags_obvious_anomaly_after_warmup()
    print("ml scorer offline tests passed")
