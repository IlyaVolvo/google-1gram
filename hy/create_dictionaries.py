#!/usr/bin/env python3
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-05

create_dictionaries.py

Create length-specific dictionary files from a combined word CSV.

Given a combined.csv file with columns: word,count,tag,definition
this script creates separate dictionary-N.txt files for each word
length N observed in the input. Each output file:
- Contains only rows whose word length is N.
- Is sorted in descending order of count (numeric).
- Uses a space instead of a comma after the word field, so lines look like:
    word count,tag,definition

Additionally, for each dictionary-N.txt file created, the script prints
one summary line to stdout reporting: N, number of words, and the count
range (min..max) for that file.

Usage:
  ./create_dictionaries.py combined.csv
  ./create_dictionaries.py --count 5 combined.csv   # skip entries with count < 5
"""

import sys
import csv
from collections import defaultdict
from typing import Dict, List, Optional


def parse_args(argv: List[str]) -> (str, Optional[int]):
    count_threshold: Optional[int] = None
    args = argv[1:]

    if args and args[0] == "--count":
        if len(args) < 3:
            sys.stderr.write(
                "Usage: create_dictionaries.py [--count N] combined.csv\n"
            )
            sys.exit(1)
        try:
            count_threshold = int(args[1])
        except ValueError:
            sys.stderr.write("--count must be an integer\n")
            sys.exit(1)
        args = args[2:]

    if len(args) != 1:
        sys.stderr.write(
            "Usage: create_dictionaries.py [--count N] combined.csv\n"
        )
        sys.exit(1)

    combined_path = args[0]
    return combined_path, count_threshold


def load_rows(path: str, count_threshold: Optional[int]) -> Dict[int, List[dict]]:
    by_length: Dict[int, List[dict]] = defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            word = row["word"]
            try:
                count = int(row["count"])
            except (ValueError, KeyError):
                # Skip rows with invalid counts
                continue

            if count_threshold is not None and count < count_threshold:
                continue

            n = len(word)
            row["count"] = count
            by_length[n].append(row)
    return by_length


def write_dictionaries(by_length: Dict[int, List[dict]]):
    for n, rows in sorted(by_length.items()):
        # Sort rows in descending count order
        rows_sorted = sorted(rows, key=lambda r: r["count"], reverse=True)
        out_name = f"dictionary-{n}.txt"

        if not rows_sorted:
            continue

        max_count = rows_sorted[0]["count"]
        min_count = rows_sorted[-1]["count"]
        num_words = len(rows_sorted)

        with open(out_name, "w", encoding="utf-8", newline="") as out:
            for r in rows_sorted:
                word = r["word"]
                count = r["count"]
                tag = r.get("tag", "")
                definition = r.get("definition", "")
                # word<space>count,tag,definition
                line = f"{word} {count},{tag},{definition}\n"
                out.write(line)

        # Report: length N, number of words, and count range
        print(f"dictionary-{n}.txt: {num_words} words, count range {min_count}..{max_count}")


def main():
    combined_path, count_threshold = parse_args(sys.argv)
    by_length = load_rows(combined_path, count_threshold)
    write_dictionaries(by_length)


if __name__ == "__main__":
    main()
