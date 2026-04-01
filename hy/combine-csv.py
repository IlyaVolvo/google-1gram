#!/usr/bin/env python3
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

combine_csv.py

Combine tagged word records with frequency counts into a single CSV.

Major functionality:
- Read word/tag/definition records from a main word-tags.csv file and
  an optional augment CSV.
- Determine allowed POS tags dynamically from a weights file and a
  minimum weight threshold.
- Filter to words whose POS tag is in the allowed set.
- Restrict to words whose length is between 4 and 9 characters (inclusive).
- Merge in counts from word-count.csv, treating missing counts as 0,
  then add 1 to each count.
- Optionally "boost" counts for words listed in a boost file by
  scaling them to five-digit integers (e.g., 3 → 30000, 241 → 24100).
- Output CSV to stdout with columns: word,count,tag,definition, with
  all words lowercased and no duplicate words.
- Report ALLOWED_TAGS and progress on stderr.

Command-line examples:
  ./combine_csv.py --weights pos-weights.txt --w 10 word-tags.csv word-count.csv > combined.csv
  ./combine_csv.py --weights pos-weights.txt --w 10 --augment augment.csv word-tags.csv word-count.csv > combined.csv
  ./combine_csv.py --weights pos-weights.txt --w 10 --boost boost-count.txt word-tags.csv word-count.csv > combined.csv
"""

import sys
import csv
import re
from typing import Dict, Iterable, TextIO, List, Set, Optional, Tuple


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


def load_pos_weights(path: str) -> Dict[str, int]:
    weights: Dict[str, int] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 2:
                continue
            tag, value = parts
            try:
                w = int(value)
            except ValueError:
                continue
            weights[tag] = w
    return weights


def build_allowed_tags(weights_path: str, min_weight: int) -> Set[str]:
    weights = load_pos_weights(weights_path)
    return {tag for tag, w in weights.items() if w >= min_weight}


def load_boost_words(path: str) -> Set[str]:
    """Support both:
    - CSV with a 'word' column, and
    - Plain comma/whitespace-separated list of words (like boost-count.txt).
    """
    words: Set[str] = set()
    with open(path, encoding="utf-8") as f:
        first = f.readline()
        if not first:
            return words

        # If first token looks like a header containing 'word', treat as CSV
        first_token = first.strip().split(",", 1)[0].lower()
        if "word" in first_token:
            f.seek(0)
            reader = csv.DictReader(f)
            for row in reader:
                w = row.get("word", "").strip()
                if w:
                    words.add(w.lower())
        else:
            # Treat file as plain list: comma- or whitespace-separated words
            text = first + f.read()
            for token in re.split(r"[\s,]+", text):
                token = token.strip()
                if token:
                    words.add(token.lower())
    return words


def parse_args(
    argv: List[str],
) -> Tuple[str, int, Optional[str], Optional[str], str, str]:
    args = argv[1:]

    weights_path: Optional[str] = None
    min_weight: Optional[int] = None
    augment_path: Optional[str] = None
    boost_path: Optional[str] = None

    i = 0
    while i < len(args) and args[i].startswith("--"):
        flag = args[i]
        if flag == "--weights":
            if i + 1 >= len(args):
                sys.stderr.write("--weights requires a filename\n")
                sys.exit(1)
            weights_path = args[i + 1]
            i += 2
        elif flag in ("--w", "--weight"):
            if i + 1 >= len(args):
                sys.stderr.write("--w requires an integer weight\n")
                sys.exit(1)
            try:
                min_weight = int(args[i + 1])
            except ValueError:
                sys.stderr.write("--w must be an integer\n")
                sys.exit(1)
            i += 2
        elif flag == "--augment":
            if i + 1 >= len(args):
                sys.stderr.write("--augment requires a filename\n")
                sys.exit(1)
            augment_path = args[i + 1]
            i += 2
        elif flag == "--boost":
            if i + 1 >= len(args):
                sys.stderr.write("--boost requires a filename\n")
                sys.exit(1)
            boost_path = args[i + 1]
            i += 2
        else:
            sys.stderr.write(f"Unknown option: {flag}\n")
            sys.exit(1)

    # Remaining args should be: word-tags.csv word-count.csv
    if len(args) - i != 2:
        sys.stderr.write(
            "Usage: combine_csv.py --weights pos-weights.txt --w N "
            "[--augment augment.csv] [--boost boost.txt] word-tags.csv word-count.csv\n"
        )
        sys.exit(1)

    if weights_path is None or min_weight is None:
        sys.stderr.write("--weights and --w are required\n")
        sys.exit(1)

    tags_path = args[i]
    counts_path = args[i + 1]
    return weights_path, min_weight, augment_path, boost_path, tags_path, counts_path


def apply_boost(count: int, boost: bool) -> int:
    if not boost:
        return count
    # Append zeros to make a 5-digit integer: 3 -> 30000, 241 -> 24100, etc.
    s = str(count)
    if len(s) >= 5:
        return count
    zeros = 5 - len(s)
    return int(s + ("0" * zeros))


def main():
    (
        weights_path,
        min_weight,
        augment_path,
        boost_path,
        tags_path,
        counts_path,
    ) = parse_args(sys.argv)

    allowed_tags = build_allowed_tags(weights_path, min_weight)
    # Print ALLOWED_TAGS once at the beginning
    print(f"ALLOWED_TAGS: {sorted(allowed_tags)}", file=sys.stderr)

    with open(counts_path, newline="", encoding="utf-8") as cf:
        counts = load_counts(cf)

    boost_words: Set[str] = set()
    if boost_path is not None:
        boost_words = load_boost_words(boost_path)

    input_files: List[str] = [tags_path]
    if augment_path is not None:
        input_files.append(augment_path)

    fieldnames = ["word", "count", "tag", "definition"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    seen_words: Set[str] = set()
    recorded = 0

    for row in iter_tagged_records(input_files):
        word_raw = row["word"].strip()
        word = word_raw.lower()

        if word in seen_words:
            continue

        tag = row["tag"].strip()
        definition = row.get("definition", "")

        if tag not in allowed_tags:
            continue
        if not (4 <= len(word) <= 9):
            continue

        base_count = counts.get(word_raw, 0)
        count = base_count + 1

        # Apply boosting if word is in boost list (compare lowercase)
        count = apply_boost(count, word in boost_words)

        writer.writerow(
            {
                "word": word,
                "count": count,
                "tag": tag,
                "definition": definition,
            }
        )

        seen_words.add(word)
        recorded += 1
        if recorded % 1000 == 0:
            # progress on the same stderr line
            print(f"\rRecorded {recorded} words...", end="", file=sys.stderr, flush=True)

    # Final newline so the shell prompt starts on a new line
    print(f"Done. Recorded {recorded} words.      ", file=sys.stderr)


if __name__ == "__main__":
    main()