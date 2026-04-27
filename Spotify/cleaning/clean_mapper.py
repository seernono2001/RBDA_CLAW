#!/usr/bin/env python3
"""
Cleaning Job — ETL Mapper
Reads raw Spotify CSV rows and outputs cleaned, normalized records.

Transformations applied:
  1. Normalize artists: lowercase, strip whitespace, remove special characters
  2. Normalize track_name: lowercase, strip, remove special characters
  3. Normalize track_genre: lowercase, strip
  4. Validate numeric fields: skip row if critical numeric fields are unparseable
  5. Drop rows where track_id, artists, or track_name are missing
  6. Flag explicit column as boolean (True/False)
  7. Output: tab-separated key=artists|track_name, value=full cleaned CSV row
     (key used by reducer for deduplication)
"""

import sys
import csv
import re

COLUMNS = [
    "row_index", "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature", "track_genre"
]

# Output columns — drop the raw row_index, it's meaningless after cleaning
OUTPUT_COLS = COLUMNS[1:]

REQUIRED_COLS = {"track_id", "artists", "track_name"}

NUMERIC_COLS = {
    "popularity", "duration_ms", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature"
}

def normalize_text(s):
    """Lowercase, strip, collapse whitespace, remove non-ASCII punctuation."""
    s = s.strip().lower()
    s = re.sub(r"[^\x00-\x7F]+", "", s)          # remove non-ASCII
    s = re.sub(r"[^\w\s,&'\-\.]", "", s)          # keep letters, digits, basic punct
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main():
    reader = csv.reader(sys.stdin)
    first_line = True
    for row in reader:
        if first_line:
            first_line = False
            continue  # skip header
        if len(row) < len(COLUMNS):
            continue  # skip malformed rows

        record = dict(zip(COLUMNS, row))

        # --- 1. Drop rows with missing required fields ---
        skip = False
        for col in REQUIRED_COLS:
            v = record[col].strip()
            if v == "" or v.lower() in ("none", "null", "nan"):
                skip = True
                break
        if skip:
            continue

        # --- 2. Validate numeric fields (drop row if critical ones are invalid) ---
        for col in NUMERIC_COLS:
            v = record[col].strip()
            if v == "" or v.lower() in ("none", "null", "nan"):
                record[col] = ""  # allow missing numeric as empty
                continue
            try:
                float(v)
            except ValueError:
                record[col] = ""  # clear unparseable numeric value

        # --- 3. Normalize text fields ---
        # Artists may be semicolon-separated — normalize each individually, rejoin
        artists_normalized = ";".join(
            normalize_text(a) for a in record["artists"].split(";") if a.strip()
        )
        record["artists"]     = artists_normalized
        record["track_name"]  = normalize_text(record["track_name"])
        record["album_name"]  = normalize_text(record["album_name"])
        record["track_genre"] = normalize_text(record["track_genre"])

        # --- 4. Normalize explicit flag ---
        explicit_val = record["explicit"].strip().lower()
        if explicit_val in ("true", "1", "yes"):
            record["explicit"] = "true"
        elif explicit_val in ("false", "0", "no"):
            record["explicit"] = "false"
        else:
            record["explicit"] = "false"  # default to false if ambiguous

        # --- 5. Build dedup key: normalized artists + track_name ---
        dedup_key = f"{record['artists']}|{record['track_name']}"

        # --- 6. Emit: key \t full cleaned CSV row (without row_index) ---
        cleaned_row = [record[col] for col in OUTPUT_COLS]
        print(f"{dedup_key}\t{','.join(cleaned_row)}")

if __name__ == "__main__":
    main()
