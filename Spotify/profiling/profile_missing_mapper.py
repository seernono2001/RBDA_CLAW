#!/usr/bin/env python3
"""
Profiling Job 1 — Missing Values
Mapper: for every field in every row, emits (column_name, MISSING) or (column_name, PRESENT).

Dataset: maharshipandya/-spotify-tracks-dataset
Columns: track_id, artists, album_name, track_name, popularity, duration_ms, explicit,
         danceability, energy, key, loudness, mode, speechiness, acousticness,
         instrumentalness, liveness, valence, tempo, time_signature, track_genre
"""

import sys
import csv

COLUMNS = [
    "row_index", "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature", "track_genre"
]

# Columns we actually care about profiling (skip the unnamed row index)
PROFILE_COLS = COLUMNS[1:]

def main():
    reader = csv.reader(sys.stdin)
    first_line = True
    for row in reader:
        if first_line:
            first_line = False
            continue  # skip header
        if len(row) < len(COLUMNS):
            for col in PROFILE_COLS:
                print(f"{col}\tMISSING")
            continue
        record = dict(zip(COLUMNS, row))
        for col in PROFILE_COLS:
            val = record[col].strip()
            if val == "" or val.lower() in ("none", "null", "nan"):
                print(f"{col}\tMISSING")
            else:
                print(f"{col}\tPRESENT")
            val = val.strip()
            if val == "" or val.lower() in ("none", "null", "nan"):
                print(f"{col}\tMISSING")
            else:
                print(f"{col}\tPRESENT")

if __name__ == "__main__":
    main()
