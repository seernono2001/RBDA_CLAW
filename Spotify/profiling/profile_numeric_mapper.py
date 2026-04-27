#!/usr/bin/env python3
"""
Profiling Job 2 — Numeric Column Statistics
Mapper: emits (column_name, value) for every numeric field that can be parsed.

Numeric columns profiled:
  popularity, duration_ms, danceability, energy, key, loudness, mode,
  speechiness, acousticness, instrumentalness, liveness, valence,
  tempo, time_signature
"""

import sys
import csv

COLUMNS = [
    "row_index", "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature", "track_genre"
]

NUMERIC_COLS = {
    "popularity", "duration_ms", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature"
}

def main():
    reader = csv.reader(sys.stdin)
    first_line = True
    for row in reader:
        if first_line:
            first_line = False
            continue
        if len(row) < len(COLUMNS):
            continue
        record = dict(zip(COLUMNS, row))
        for col in NUMERIC_COLS:
            val = record[col].strip()
            if val == "" or val.lower() in ("none", "null", "nan"):
                continue
            try:
                float(val)
                print(f"{col}\t{val}")
            except ValueError:
                continue  # Skip unparseable values

if __name__ == "__main__":
    main()
