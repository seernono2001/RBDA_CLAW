#!/usr/bin/env python3
"""
Profiling Job 1 — Missing Values
Reducer: counts MISSING and PRESENT per column, outputs a readable summary table.
"""

import sys
from collections import defaultdict

def main():
    counts = defaultdict(lambda: {"MISSING": 0, "PRESENT": 0})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        col, status = parts
        counts[col][status] += 1

    # Preserve logical column order
    col_order = [
        "track_id", "artists", "album_name", "track_name", "popularity",
        "duration_ms", "explicit", "danceability", "energy", "key",
        "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "time_signature", "track_genre"
    ]
    # Note: row_index is excluded from profiling output

    print(f"{'Column':<22} {'Total':>8} {'Missing':>8} {'Missing%':>10}")
    print("-" * 52)
    for col in col_order:
        if col not in counts:
            continue
        total = counts[col]["MISSING"] + counts[col]["PRESENT"]
        missing = counts[col]["MISSING"]
        pct = (missing / total * 100) if total > 0 else 0.0
        flag = " <-- CHECK" if pct > 5 else ""
        print(f"{col:<22} {total:>8} {missing:>8} {pct:>9.2f}%{flag}")

if __name__ == "__main__":
    main()
