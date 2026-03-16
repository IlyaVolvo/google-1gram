#!/usr/bin/env python3
"""
Design: Aram
Coding: Perplexity
Date: 2026-π

split-wordforms.py for Armenian: ել ում ած եր ով իկ ող

Takes a text file with one record per line in the format:

    <word><space><text>

and a list of suffixes. It moves records whose <word> ends with these suffixes
into separate files named:

    <name>-<suffix>.txt

where <name>.txt is the original input file. Records that do not match any
suffix remain in the original file. At the end, it prints a report of the form:

    <filename>.txt - <number of records>

for all files involved, including the updated input file.
"""

import argparse
import os
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(
        description="Split wordforms into separate files based on word suffixes."
    )
    parser.add_argument(
        "input_file",
        help="Input text file with lines of the form '<word> <text>'.",
    )
    parser.add_argument(
        "--suffix",
        "-s",
        dest="suffixes",
        nargs="+",
        required=True,
        help="One or more suffixes to split on (e.g. --suffix ing ed s).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_file = args.input_file
    suffixes = args.suffixes

    base, ext = os.path.splitext(input_file)
    if not ext:
        ext = ".txt"

    # Prepare output file names for each suffix
    suffix_to_path = {
        suf: f"{base}-{suf}{ext}" for suf in suffixes
    }

    # First pass: read all lines and classify them
    # mapping suffix -> list of lines, and list of remaining lines
    matched_lines = defaultdict(list)
    remaining_lines = []

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")
            if not stripped:
                # empty line goes back to remaining_lines unchanged
                remaining_lines.append(line)
                continue

            # Split only on first space: word, rest_of_text
            parts = stripped.split(" ", 1)
            word = parts[0]

            moved = False
            for suf in suffixes:
                if word.endswith(suf):
                    matched_lines[suf].append(line)
                    moved = True
                    break
            if not moved:
                remaining_lines.append(line)

    # Write out files for each suffix
    for suf, path in suffix_to_path.items():
        lines = matched_lines.get(suf, [])
        # Overwrite if exists; create otherwise
        with open(path, "w", encoding="utf-8") as out_f:
            out_f.writelines(lines)

    # Rewrite the original file with the remaining lines
    with open(input_file, "w", encoding="utf-8") as f:
        f.writelines(remaining_lines)

    # Count records in all involved files
    file_counts = {}

    # Count in original file
    with open(input_file, "r", encoding="utf-8") as f:
        file_counts[input_file] = sum(1 for _ in f)

    # Count in each suffix file
    for suf, path in suffix_to_path.items():
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                file_counts[path] = sum(1 for _ in f)
        else:
            file_counts[path] = 0

    # Print report
    for fname, count in sorted(file_counts.items()):
        print(f"{fname} - {count} records")


if __name__ == "__main__":
    main()
