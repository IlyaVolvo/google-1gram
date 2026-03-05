#!/usr/bin/env python3
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

combine_csv.py

Combine tagged word records with frequency counts into a single CSV.

Major functionality:
- Read word/tag/definition records from a main word-tags.csv file.
- Optionally augment these records with an additional CSV passed via --augment.
- Filter to words whose POS tag is one of: NOUN, PRON, VERB, NUM, MOD, ADP.
- Further restrict to words whose length is between 4 and 9 characters (inclusive).
- Merge in counts from word-count.csv, then add 1 to each count; if a word is
  not present in word-count.csv its base count is treated as 0, so it appears
  with count 1 in the output.
- Output CSV to stdout with columns: word,count,tag,definition, with all words
  lowercased.
- Skip duplicate words so each word appears at most once in the output.

Command-line examples:
  ./combine_csv.py word-tags.csv word-count.csv > combined.csv
  ./combine_csv.py --augment augment-records.csv word-tags.csv word-count.csv > combined.csv
"""

import sys
import csv
from typing import Dict, Iterable, TextIO, List

ALLOWED_TAGS = {"NOUN", "PRON", "VERB", "NUM", "MOD", "ADP"}


def load_counts(f: TextIO) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    reader = csv.DictReader(f)
    for row in reader:
        word = row["word"].strip()
        try:
            count = int(row["count"])
        except (ValueError, KeyError):
            continue
        counts[word] = count
    return counts


def iter_tagged_records(files: List[str]) -> Iterable[Dict[str, str]]:
    for path in files:
        with open(path, newline="", encoding="utf-8") as tf:
            reader = csv.DictReader(tf)
            for row in reader:
                yield row


def parse_args(argv: List[str]):
    augment_path = None
    args = list(argv[1:])

    if args and args[0] == "--augment":
        if len(args) < 3:
            sys.stderr.write(
                "Usage: combine_csv.py [--augment augment.csv] word-tags.csv word-count.csv\n"
            )
            sys.exit(1)
        augment_path = args[1]
        args = args[2:]

    if len(args) != 2:
        sys.stderr.write(
            "Usage: combine_csv.py [--augment augment.csv] word-tags.csv word-count.csv\n"
        )
        sys.exit(1)

    tags_path, counts_path = args
    return augment_path, tags_path, counts_path


def main():
    augment_path, tags_path, counts_path = parse_args(sys.argv)

    with open(counts_path, newline="", encoding="utf-8") as cf:
        counts = load_counts(cf)

    input_files = [tags_path]
    if augment_path is not None:
        input_files.append(augment_path)

    fieldnames = ["word", "count", "tag", "definition"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    recorded = 0
    seen_words = set()  # track words already written

    for row in iter_tagged_records(input_files):
        word_raw = row["word"].strip()
        word = word_raw.lower()

        # skip if this word is already recorded
        if word in seen_words:
            continue

        tag = row["tag"].strip()
        definition = row.get("definition", "")

        if tag not in ALLOWED_TAGS:
            continue
        if not (4 <= len(word) <= 9):
            continue

        base_count = counts.get(word_raw, 0)
        count = base_count + 1

        writer.writerow({
            "word": word,
            "count": count,
            "tag": tag,
            "definition": definition,
        })

        seen_words.add(word)
        recorded += 1
        if recorded % 100 == 0:
           sys.stderr.write(f"\rRecorded {recorded} words...")
           sys.stderr.flush()

    print(f"Done. Recorded {recorded} words.", file=sys.stderr)


if __name__ == "__main__":
    main()
