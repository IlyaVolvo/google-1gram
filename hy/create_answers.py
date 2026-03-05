#!/usr/bin/env python3
"""create_answers.py

Create answers-N.txt files from dictionary-N.txt files by selecting
records based on percentage ranges.

Given a dictionary-N.txt file (lines like: word count,tag,definition),
this script:
- Skips the first K% of records (by line order).
- Takes the next M% of records into answers-N.txt.
- Writes them in the same format as the input.
- Prints a summary line with number of entries and count range.

Usage:
  ./create_answers.py --skip K --keep M dictionary-N.txt
Example:
  ./create_answers.py --skip 10 --keep 20 dictionary-5.txt
"""

import sys
from typing import List, Tuple


def parse_args(argv: List[str]) -> Tuple[int, int, str]:
    args = argv[1:]
    if len(args) != 5 or args[0] != "--skip" or args[2] != "--keep":
        sys.stderr.write(
            "Usage: create_answers.py --skip K --keep M dictionary-N.txt\n"
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

    dict_path = args[4]
    return skip_pct, keep_pct, dict_path


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
    skip_pct, keep_pct, dict_path = parse_args(sys.argv)

    lines = load_lines(dict_path)
    selected = select_range(lines, skip_pct, keep_pct)

    # Derive output name assuming dictionary-N.txt → answers-N.txt
    out_path = dict_path.replace("dictionary-", "answers-")

    if not selected:
        # still create an empty file
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
