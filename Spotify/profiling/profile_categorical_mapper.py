#!/usr/bin/env python3
"""
Profiling Job 3 — Categorical Column Profiling
Mapper: emits (column_name|value, 1) for categorical columns.
Helps identify unique values, top genres, top artists, etc.

Categorical columns profiled: artists, track_genre, explicit
"""

import sys
import csv

COLUMNS = [
    "row_index", "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature", "track_genre"
]

CATEGORICAL_COLS = ["artists", "track_genre", "explicit"]

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
        for col in CATEGORICAL_COLS:
            val = record[col].strip()
            if val == "" or val.lower() in ("none", "null", "nan"):
                print(f"{col}|__MISSING__\t1")
                continue
            if col == "artists":
                # Artists can be semicolon-separated (e.g. "Ingrid Michaelson;ZAYN")
                # Emit each artist individually for accurate unique-artist counting
                for artist in val.split(";"):
                    artist = artist.strip().lower()
                    if artist:
                        print(f"{col}|{artist}\t1")
            else:
                print(f"{col}|{val.lower()}\t1")

if __name__ == "__main__":
    main()
