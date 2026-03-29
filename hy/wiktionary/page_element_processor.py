#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

page_element_processor.py

High-level driver for extracting Armenian Wiktionary entries from a
Wiktionary XML dump, using the refactored, modular pipeline:

  - Pre-processing: text_pre_processors.pp1/pp2/pp3 (structural fixes).
  - POS extraction and description building: text_processor + description_processor.
  - Error classification into filtered-extract.csv with correct 'filter'
    column values ('skipped', 'REDIRECT', 'no tag', unmapped POS key, 'no desc').
"""

import debug_utils
import sys
import re
import csv
import argparse
import os
import xml.etree.ElementTree as ET
from typing import Dict
import text_pre_processors as tpp
import text_processor as tp
import description_processor as dp

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

ARMENIAN_TITLE_RE = re.compile(r"^[\u0531-\u0556\u0561-\u0587]+$")
REDIRECT_RE = re.compile(r"#REDIRECT", re.IGNORECASE)

POS_MAP: Dict[str, str] = {}
POS_WEIGHTS: Dict[str, int] = {}


def load_pos_map(path: str) -> Dict[str, str]:
    pos_map: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                key = parts[0].strip()
                tag = parts[-1].strip()
                if key and tag:
                    pos_map[key] = tag
    except OSError as e:
        sys.stderr.write(f"Warning: could not read POS map file '{path}': {e}\n")
    return pos_map


def load_pos_weights(path: str) -> Dict[str, int]:
    weights: Dict[str, int] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                tag = parts[0].strip()
                try:
                    w = int(parts[-1])
                except ValueError:
                    continue
                weights[tag] = w
    except OSError as e:
        sys.stderr.write(f"Warning: could not read POS weights file '{path}': {e}\n")
    return weights


def normalize_title(title: str) -> str:
    if not title:
        return ""
    return title.replace("\uFEFF", "").replace("\u200B", "")


def normalized_length(word: str) -> int:
    return len(word.replace("և", "եւ"))


def is_armenian_title(title: str) -> bool:
    if not title:
        return False
    return ARMENIAN_TITLE_RE.match(title) is not None


def iter_pages(xml_path: str):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    m = re.match(r"\{.*\}", root.tag)
    ns = m.group(0) if m else ""
    page_tag = ns + "page"
    title_tag = ns + "title"
    text_tag = ns + "text"

    idx = 0
    for page in root.iter(page_tag):
        idx += 1
        title_el = page.find("./" + title_tag)
        text_el = page.find(".//" + text_tag)
        yield idx, title_el, text_el


def load_skip_file(path: str):
    if not path:
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as e:
        sys.stderr.write(f"Warning: could not read skip file '{path}': {e}\n")
        return set()
    skip = set()
    for token in content.split(","):
        token = token.strip()
        if token:
            skip.add(token)
    return skip


def parse_args():
    p = argparse.ArgumentParser(
        description="Extract Armenian Wiktionary items (word, POS, definition) to CSV."
    )

    p.add_argument("--min", type=int, default=4,
                   help="Minimum normalized word length (default: 4).")
    p.add_argument("--max", type=int, default=9,
                   help="Maximum normalized word length (default: 9).")
    p.add_argument(
        "--skip",
        type=str,
        default="",
        help="Path to text file with comma-separated words to skip.",
    )
    p.add_argument(
        "--filter",
        type=str,
        default="",
        help="Comma-separated values passed as filter_string to description_processor.",
    )
    p.add_argument(
        "--desc-trigger",
        type=str,
        default="",
        help="Comma-separated values passed as trigger_second_bullet to description_processor.",
    )
    p.add_argument(
        "--single",
        type=str,
        default="n",
        help="If 'y', output at most one record per word, choosing the POS "
             "with highest weight; otherwise output all POS records (default: n).",
    )
    p.add_argument(
        "--nopp",
        type=str,
        default="",
        help="Comma-separated list of pre-processors to disable (e.g. 'pp1,pp3').",
    )
    # NEW: debug flag
    p.add_argument(
        "--debug",
        type=str,
        default="",
        help="enable debug logging to debug.txt (any non-empty word)",
    )

    p.add_argument("xml_path", help="Path to Armenian Wiktionary XML dump.")
    return p.parse_args()

def main():
    global POS_MAP, POS_WEIGHTS

    args = parse_args()
    single_mode = (args.single.lower() == "y")

    debug_target = normalize_title(args.debug).strip() if args.debug else ""

    pos_map_path = os.path.join(SCRIPT_DIR, "pos-map.txt")
    POS_MAP = load_pos_map(pos_map_path)
    tp.POS_MAP = POS_MAP

    pos_weights_path = os.path.join(SCRIPT_DIR, "pos-weights.txt")
    POS_WEIGHTS = load_pos_weights(pos_weights_path)
    tp.POS_WEIGHTS = POS_WEIGHTS

    dp.FILTER_STRING = args.filter or None
    dp.DESC_TRIGGER = args.desc_trigger or None
    dp.USE_COUNT = False
    dp.COUNT_VALUE = None

    skip_set = load_skip_file(args.skip.strip()) if args.skip.strip() else set()
    disabled_pp = {name.strip() for name in args.nopp.split(",") if name.strip()}

    out_writer = csv.writer(sys.stdout)
    out_writer.writerow(["word", "tag", "definition"])

    filtered_path = os.path.join(SCRIPT_DIR, "filtered-extract.csv")
    filtered_file = open(filtered_path, "w", encoding="utf-8", newline="")
    filtered_writer = csv.writer(filtered_file)
    filtered_writer.writerow(["word", "tag", "filter", "definition"])

    total_titles = 0
    extracted_entries = 0
    filtered_entries = 0
    non_armenian_titles = 0
    redirect_titles = 0

    for idx, title_el, text_el in iter_pages(args.xml_path):
        total_titles = idx

        if total_titles % 100 == 0:
            sys.stderr.write(f"\rProcessed titles: {total_titles}")
            sys.stderr.flush()

        if title_el is None:
            continue

        raw_title = title_el.text or ""
        title = normalize_title(raw_title).strip()

        if not is_armenian_title(title):
            non_armenian_titles += 1
            continue

        if debug_target and title == debug_target:
            debug_utils.DEBUG_WORD = title
        else:
            debug_utils.DEBUG_WORD = ""

        if title in skip_set:
            filtered_writer.writerow([title, "", "skipped", ""])
            filtered_entries += 1
            continue

        if text_el is None or not text_el.text:
            filtered_writer.writerow([title, "", "no tag", ""])
            filtered_entries += 1
            continue

        text = text_el.text

        if REDIRECT_RE.search(text):
            redirect_titles += 1
            filtered_writer.writerow([title, "", "REDIRECT", ""])
            filtered_entries += 1
            continue

        if not (args.min <= normalized_length(title) <= args.max):
            continue

        # Pre-processing (pp1, pp2, pp3)
        for name, func in tpp.PRE_PROCESSORS.items():
            if name in disabled_pp:
                continue
            text = func(text)

        # Detailed POS info after pre-processing
        details = tp.extract_pos_descriptions_with_details(text)

        # Mapped + non-empty descriptions → main CSV
        mapped_nonempty = [
            (full_key, tag, desc)
            for (full_key, mapped, tag, desc) in details
            if mapped and desc
        ]

        if mapped_nonempty:
            if single_mode:
                selected = tp.select_single_by_weight(mapped_nonempty)
            else:
                selected = mapped_nonempty

            for full_key, tag, desc in selected:
                out_writer.writerow([title, tag, desc])
                extracted_entries += 1
            continue

        # No mapped+non-empty descriptions → filtered-extract classification

        full_keys = tp.extract_pos_keys(text)
        if not full_keys:
            filtered_writer.writerow([title, "", "no tag", ""])
            filtered_entries += 1
            continue

        any_mapped = any(mapped for (_, mapped, _, _) in details)

        if not any_mapped:
            # Unmapped POS: filter column is the first full_key
            first_key = full_keys[0]
            filtered_writer.writerow([title, first_key, first_key, ""])
            filtered_entries += 1
            continue

        # Mapped POS exist, but all mapped desc are empty → 'no desc'
        first_mapped_tag = ""
        for full_key, mapped, tag, desc in details:
            if mapped:
                first_mapped_tag = tag
                break

        filtered_writer.writerow([title, first_mapped_tag, "no desc", ""])
        filtered_entries += 1

    sys.stderr.write(f"\rProcessed titles: {total_titles}\n")
    sys.stderr.write(f"Extracted entries - {extracted_entries}\n")
    sys.stderr.write(f"Skipped/filtered entries - {filtered_entries}\n")
    sys.stderr.write(f"Titles with non-Armenian characters - {non_armenian_titles}\n")
    sys.stderr.write(f"Titles ignored because of redirects - {redirect_titles}\n")
    sys.stderr.flush()
    filtered_file.close()


if __name__ == "__main__":
    main()