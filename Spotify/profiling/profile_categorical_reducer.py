#!/usr/bin/env python3
"""
Profiling Job 3 — Categorical Column Profiling
Reducer: counts occurrences of each value per column.
Outputs unique value count and top-10 most frequent values per column.
"""

import sys
from collections import defaultdict

def main():
    counts = defaultdict(lambda: defaultdict(int))

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        key, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue
        if "|" not in key:
            continue
        col, val = key.split("|", 1)
        counts[col][val] += count

    for col in sorted(counts.keys()):
        val_counts = counts[col]
        unique_count = len(val_counts)
        total = sum(val_counts.values())
        print(f"\n{'='*55}")
        print(f"Column: {col}")
        print(f"  Unique values : {unique_count}")
        print(f"  Total records : {total}")
        print(f"  Top 10 values:")
        top10 = sorted(val_counts.items(), key=lambda x: -x[1])[:10]
        for val, cnt in top10:
            pct = cnt / total * 100
            print(f"    {val:<35} {cnt:>7}  ({pct:.2f}%)")

if __name__ == "__main__":
    main()
