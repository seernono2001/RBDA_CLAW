#!/usr/bin/env python3
"""
Cleaning Job — Deduplication Reducer
Receives rows grouped by the same (artists|track_name) key.
Keeps only the first occurrence (highest popularity if multiple exist).

Output: cleaned CSV rows (no key prefix), with header on first line.
"""

import sys

COLUMNS = [
    "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature", "track_genre"
]

# popularity is index 4 in the output (row_index already dropped by mapper)
POPULARITY_IDX = 4

def parse_popularity(row_str):
    """Extract popularity value for tie-breaking."""
    parts = row_str.split(",")
    if len(parts) > POPULARITY_IDX:
        try:
            return int(parts[POPULARITY_IDX])
        except ValueError:
            pass
    return -1

def main():
    # Print header
    print(",".join(COLUMNS))

    current_key = None
    best_row = None
    best_pop = -1

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        tab_idx = line.find("\t")
        if tab_idx == -1:
            continue

        key = line[:tab_idx]
        row_str = line[tab_idx + 1:]

        if key != current_key:
            # Emit best row for previous key
            if best_row is not None:
                print(best_row)
            current_key = key
            best_row = row_str
            best_pop = parse_popularity(row_str)
        else:
            # Keep the row with highest popularity (richer data)
            pop = parse_popularity(row_str)
            if pop > best_pop:
                best_row = row_str
                best_pop = pop

    # Emit last group
    if best_row is not None:
        print(best_row)

if __name__ == "__main__":
    main()
