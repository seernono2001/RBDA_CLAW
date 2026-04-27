#!/usr/bin/env python3
"""
Profiling Job 2 — Numeric Column Statistics
Reducer: computes min, max, mean, count, and std deviation per numeric column.
"""

import sys
import math
from collections import defaultdict

def main():
    stats = defaultdict(lambda: {"count": 0, "sum": 0.0, "sum_sq": 0.0,
                                  "min": float("inf"), "max": float("-inf")})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        col, val_str = parts
        try:
            val = float(val_str)
        except ValueError:
            continue

        s = stats[col]
        s["count"] += 1
        s["sum"] += val
        s["sum_sq"] += val * val
        s["min"] = min(s["min"], val)
        s["max"] = max(s["max"], val)

    col_order = [
        "popularity", "duration_ms", "danceability", "energy", "key",
        "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "time_signature"
    ]

    print(f"{'Column':<22} {'Count':>8} {'Min':>10} {'Max':>10} {'Mean':>10} {'StdDev':>10}")
    print("-" * 74)
    for col in col_order:
        if col not in stats:
            continue
        s = stats[col]
        n = s["count"]
        if n == 0:
            continue
        mean = s["sum"] / n
        variance = (s["sum_sq"] / n) - (mean ** 2)
        std = math.sqrt(max(variance, 0))
        print(f"{col:<22} {n:>8} {s['min']:>10.3f} {s['max']:>10.3f} {mean:>10.3f} {std:>10.3f}")

if __name__ == "__main__":
    main()
