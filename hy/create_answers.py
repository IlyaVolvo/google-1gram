#!/usr/bin/env python3
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-05

create_answers.py

Create answers-N.txt files from dictionary-N.txt files by selecting
records based on percentage ranges, with optional ignoring of words
listed in a separate file.

Input:
  - dictionary-N.txt: lines like
        word count,tag,definition

Behavior:
  - Skip the first K% of records (by line order).
  - Take the next M% of records into answers-N.txt.
  - Optionally, if --ignore name.txt is provided, do not move any
    records whose word appears in name.txt (which has the same
    line format as answers-N.txt: word count,tag,definition).
  - Write selected records in the same format as the input.
  - Print a summary line with number of entries and count range
    for answers-N.txt.

Usage:
  ./create_answers.py --skip K --keep M dictionary-N.txt
  ./create_answers.py --skip K --keep M --ignore used-N.txt dictionary-N.txt
Example:
  ./create_answers.py --skip 10 --keep 20 dictionary-5.txt
  ./create_answers.py --skip 10 --keep 20 --ignore used-5.txt dictionary-5.txt
"""

import sys
from typing import List, Tuple, Optional, Set


def parse_args(argv: List[str]) -> Tuple[int, int, Optional[str], str]:
    args = argv[1:]

    # Expected patterns:
    #   --skip K --keep M dictionary-N.txt
    #   --skip K --keep M --ignore name.txt dictionary-N.txt
    if len(args) not in (5, 7):
        sys.stderr.write(
            "Usage: create_answers.py --skip K --keep M [--ignore name.txt] dictionary-N.txt\n"
        )
        sys.exit(1)

    if args[0] != "--skip" or args[2] != "--keep":
        sys.stderr.write(
            "Usage: create_answers.py --skip K --keep M [--ignore name.txt] dictionary-N.txt\n"
        )
        sys.exit(1)

    try:
        skip_pct = int(args[1])
        keep_pct = int(args[3])
    except ValueError:
        sys.stderr.write("K and M must be integers (percentages)\n")
        sys.exit(1)

    if skip_pct < 0 or keep_pct < 0 or skip_pct > 100 or keep_pct > 100:
        sys.stderr.write("K and M must be between 0 and 100\n")
        sys.exit(1)

    ignore_path: Optional[str] = None

    if len(args) == 5:
        dict_path = args[4]
    else:
        # len(args) == 7
        if args[4] != "--ignore":
            sys.stderr.write(
                "Usage: create_answers.py --skip K --keep M [--ignore name.txt] dictionary-N.txt\n"
            )
            sys.exit(1)
        ignore_path = args[5]
        dict_path = args[6]

    return skip_pct, keep_pct, ignore_path, dict_path


def load_lines(path: str) -> List[str]:
    with open(path, encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]
    return [line for line in lines if line.strip()]


def parse_count(line: str) -> int:
    # line format: word<space>count,tag,definition
    # split once on space, then on comma
    try:
        _word, rest = line.split(" ", 1)
        count_str = rest.split(",", 1)[0]
        return int(count_str)
    except Exception:
        return 0


def extract_word(line: str) -> str:
    # line format: word<space>count,tag,definition
    try:
        word, _rest = line.split(" ", 1)
        return word
    except ValueError:
        return ""


def load_ignore_words(path: str) -> Set[str]:
    words: Set[str] = set()
    for line in load_lines(path):
        w = extract_word(line)
        if w:
            words.add(w)
    return words


def select_range(lines: List[str], skip_pct: int, keep_pct: int) -> List[str]:
    n = len(lines)
    if n == 0 or keep_pct == 0:
        return []

    start_index = int(n * skip_pct / 100)
    end_index = start_index + int(n * keep_pct / 100)
    if start_index > n:
        start_index = n
    if end_index > n:
        end_index = n
    return lines[start_index:end_index]


def main():
    skip_pct, keep_pct, ignore_path, dict_path = parse_args(sys.argv)

    lines = load_lines(dict_path)
    selected = select_range(lines, skip_pct, keep_pct)

    # Optionally filter out words present in ignore file
    if ignore_path is not None:
        ignore_words = load_ignore_words(ignore_path)
        selected = [ln for ln in selected if extract_word(ln) not in ignore_words]

    # Derive output name assuming dictionary-N.txt → answers-N.txt
    out_path = dict_path.replace("dictionary-", "answers-")

    if not selected:
        with open(out_path, "w", encoding="utf-8") as out:
            pass
        print(f"{out_path}: 0 entries, count range n/a")
        return

    counts = [parse_count(line) for line in selected]
    max_count = max(counts)
    min_count = min(counts)

    with open(out_path, "w", encoding="utf-8") as out:
        for line in selected:
            out.write(line + "\n")

    print(f"{out_path}: {len(selected)} entries, count range {min_count}..{max_count}")


if __name__ == "__main__":
    main()
