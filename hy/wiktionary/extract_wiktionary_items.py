#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

extract_wiktionary_items wiktionary.xml

Extract Armenian Wiktionary entries (word, POS tag, definition)
from a Wiktionary XML dump and write them to CSV.

Features:
- Process only titles that are strings of Armenian letters; titles containing
  non-Armenian characters are ignored and counted separately.
- Ignore pages whose text contains '#REDIRECT' (case-insensitive) and report
  how many were ignored for this reason.
- Detect POS from Armenian POS templates anywhere inside the page <text>.
- POS resolution priority:
  * Collect all exact {{-hy-<x>-}} templates in the page.
  * If any of those <x> values are keys in POS_MAP (loaded from pos-map.txt),
    choose the first such template and use its tag for the whole page.
  * If no such exact-match template exists:
      - Look for any template matching {{*hy*<x>*}}:
        · If <x> can be normalized to a POS_MAP key (-hy-<x>-), use its tag.
        · Otherwise assign tag 'XXX-<x>'.
      - If no {{*hy*<x>*}} template exists at all, tag the word 'XXX'.
- POS_MAP is loaded at runtime from a text file pos-map.txt located next to
  this script. Empty lines and lines starting with '#' are ignored.
- Collect raw definition text as the concatenation of all lines in <text>
  starting with '#', with bullets containing any skip word removed.
- Build the final definition string using:
    build_description(definition,
                      count=None,
                      filter_string=<--filter values>,
                      trigger_second_bullet=<--desc-trigger values>)
  from description_utils.py.
  * Bullets containing any filter_string word (e.g. 'հնց') are dropped inside
    description_utils before choosing the description bullet.
  * If build_description returns an empty string, the entry is skipped.
- Filter words by length using a normalized form where 'և' is replaced by 'եւ'.
- Skip/skip-file handling:
  * A skip file (comma-separated words) excludes titles whose word is in the file.
  * Definition bullet items (lines starting with '#') that contain any skip word
    are removed from that word's raw definition before build_description.
- Skipped/filtered output:
  * All skipped/filtered entries are written into filtered-extract.csv.
  * Columns: word, tag, filter, definition.
  * The "filter" column is 'skipped' (for skip-file, length, no defs, or empty description).
- Progress reporting:
  * Progress is shown as the number of input words (<title> elements) processed.
  * At the end, the script prints:
    - total titles processed,
    - extracted entries,
    - skipped/filtered entries,
    - titles with non-Armenian characters,
    - titles ignored because of redirects.
"""

import sys
import re
import csv
import argparse
import os
import xml.etree.ElementTree as ET

# Import build_description from parent folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PARENT_DIR)
from description_utils import build_description  # type: ignore


def load_pos_map(pos_map_path: str) -> dict:
    """
    Load POS mapping from a text file.

    Expected format (one mapping per line):
        -hy-ած-  ADJ
        -hy-գո-  NOUN

    Lines starting with '#' or empty lines are ignored.
    The first token is the key, the last token on the line is the tag.
    """
    pos_map: dict[str, str] = {}
    try:
        with open(pos_map_path, "r", encoding="utf-8") as f:
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
        sys.stderr.write(
            f"Warning: could not read POS map file '{pos_map_path}': {e}\n"
        )
    return pos_map


# This will be set in main()
POS_MAP: dict[str, str] = {}

POS_TPL_EXACT_RE = re.compile(r"\{\{(-hy-[^}]+)\}\}")
POS_TPL_LOOSE_RE = re.compile(r"\{\{[^}]*hy(?P<x>[^}|]*)[^}]*\}\}")
DEF_RE = re.compile(r"^#\s*.+")
ARMENIAN_TITLE_RE = re.compile(r"^[\u0531-\u0556\u0561-\u0587]+$")
REDIRECT_RE = re.compile(r"#REDIRECT", re.IGNORECASE)


def normalize_for_length(word: str) -> str:
    return word.replace("և", "եւ")


def is_armenian_title(title: str) -> bool:
    if not title:
        return False
    return ARMENIAN_TITLE_RE.match(title) is not None


def resolve_pos_tag_whole_text(text: str) -> str:
    """Resolve POS tag using templates in the full text and POS_MAP."""
    # 1) exact {{-hy-<x>-}} templates
    exact_matches = POS_TPL_EXACT_RE.findall(text)
    if exact_matches:
        for key in exact_matches:
            if key in POS_MAP:
                return POS_MAP[key]
    # 2) loose {{*hy*<x>*}} templates
    loose_matches = list(POS_TPL_LOOSE_RE.finditer(text))
    if loose_matches:
        m = loose_matches[0]
        x = (m.group("x") or "").strip()
        key_candidate = f"-hy-{x}-" if x else ""
        if key_candidate in POS_MAP:
            return POS_MAP[key_candidate]
        if x:
            return f"XXX-{x}"
        return "XXX"
    # 3) no hy templates at all
    return "XXX"


def extract_definition_from_text(text: str, skip_set):
    """
    Any line starting with '#' is considered a definition bullet.
    Bullets containing any skip word are removed.
    """
    defs = []
    for line in text.splitlines():
        line = line.rstrip()
        if DEF_RE.match(line):
            if any(sw in line for sw in skip_set):
                continue
            defs.append(line)
    if not defs:
        return ""
    return " ".join(defs)


def extract_from_page(title_el, text_el, skip_set):
    """
    Extract (word, tag, raw_definition) for a single <page> element.
    """
    if title_el is None or text_el is None or not text_el.text:
        return "NODEFS", None

    word = (title_el.text or "").strip()
    text = text_el.text

    if REDIRECT_RE.search(text):
        return "REDIRECT", None

    tag = resolve_pos_tag_whole_text(text)
    definition_str = extract_definition_from_text(text, skip_set)
    if not definition_str:
        return "NODEFS", None

    return "OK", (word, tag, definition_str)


def iter_pages(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    m = re.match(r"\{.*\}", root.tag)
    ns = m.group(0) if m else ""
    page_tag = ns + "page"
    title_tag = ns + "title"
    text_tag = ns + "text"

    title_count = 0
    for page in root.iter(page_tag):
        title_el = page.find("./" + title_tag)
        text_el = page.find(".//" + text_tag)
        title_count += 1
        yield title_count, title_el, text_el


def load_skip_file(path: str):
    if not path:
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as e:
        sys.stderr.write(f"Warning: could not read skip file '{path}': {e}\n")
        return set()
    skip_set = set()
    for w in content.split(","):
        w = w.strip()
        if w:
            skip_set.add(w)
    return skip_set


def parse_args():
    p = argparse.ArgumentParser(
        description="Extract Armenian Wiktionary items (word, POS, definition) to CSV."
    )
    p.add_argument("--min", type=int, default=4,
                   help="Minimum word length (normalized: 'և'→'եւ'). Default: 4.")
    p.add_argument("--max", type=int, default=9,
                   help="Maximum word length (normalized: 'և'→'եւ'). Default: 9.")
    p.add_argument(
        "--skip",
        type=str,
        default="",
        help="Path to a text file with a comma-separated list of words to exclude."
    )
    p.add_argument(
        "--filter",
        type=str,
        default="",
        help=(
            "Comma-separated list of words; passed as filter_string to "
            "build_description so bullets containing them are dropped."
        ),
    )
    p.add_argument(
        "--desc-trigger",
        type=str,
        default="",
        help=(
            "Comma-separated trigger words for choosing the second bullet when "
            "building description; passed as trigger_second_bullet to "
            "build_description."
        ),
    )
    p.add_argument("xml_path", help="Path to Armenian Wiktionary XML dump.")
    return p.parse_args()


def main():
    global POS_MAP

    args = parse_args()

    # Load POS_MAP from pos-map.txt in the same directory as this script
    pos_map_path = os.path.join(SCRIPT_DIR, "pos-map.txt")
    POS_MAP = load_pos_map(pos_map_path)

    skip_set = load_skip_file(args.skip.strip()) if args.skip.strip() else set()

    out_writer = csv.writer(sys.stdout)
    out_writer.writerow(["word", "tag", "definition"])

    filtered_path = "filtered-extract.csv"
    filtered_file = open(filtered_path, "w", encoding="utf-8", newline="")
    filtered_writer = csv.writer(filtered_file)
    filtered_writer.writerow(["word", "tag", "filter", "definition"])

    total_titles = 0
    extracted_count = 0
    filtered_count = 0
    non_armenian_titles = 0
    redirect_titles = 0

    for title_idx, title_el, text_el in iter_pages(args.xml_path):
        total_titles = title_idx

        if total_titles % 100 == 0:
            sys.stderr.write(f"\rProcessed titles: {total_titles}")
            sys.stderr.flush()

        if title_el is None:
            continue

        word = (title_el.text or "").strip()

        # Armenian-only titles
        if not is_armenian_title(word):
            non_armenian_titles += 1
            continue

        # Skip-file titles
        if word in skip_set:
            filtered_writer.writerow([word, "", "skipped", ""])
            filtered_count += 1
            continue

        status, payload = extract_from_page(title_el, text_el, skip_set)

        if status == "REDIRECT":
            redirect_titles += 1
            continue

        if status == "NODEFS" or payload is None:
            filtered_writer.writerow([word, "", "skipped", ""])
            filtered_count += 1
            continue

        word_rec, tag, raw_definition = payload

        # Length filter
        norm_word = normalize_for_length(word_rec)
        if not (args.min <= len(norm_word) <= args.max):
            filtered_writer.writerow([word_rec, tag, "skipped", raw_definition])
            filtered_count += 1
            continue

        # Build final definition via description_utils; no count/frequency here
        final_def = build_description(
            raw_definition,
            count=None,
            filter_string=args.filter if args.filter.strip() else None,
            trigger_second_bullet=args.desc_trigger if args.desc_trigger.strip() else None,
        )

        # Skip entry if build_description returns empty
        if not final_def:
            filtered_writer.writerow([word_rec, tag, "skipped", raw_definition])
            filtered_count += 1
            continue

        out_writer.writerow([word_rec, tag, final_def])
        extracted_count += 1

    sys.stderr.write(f"\rProcessed titles: {total_titles}\n")
    sys.stderr.write(f"Extracted entries: {extracted_count}\n")
    sys.stderr.write(f"Skipped/filtered entries: {filtered_count}\n")
    sys.stderr.write(f"Titles with non-Armenian characters: {non_armenian_titles}\n")
    sys.stderr.write(f"Titles ignored because of redirects: {redirect_titles}\n")
    sys.stderr.flush()
    filtered_file.close()


if __name__ == "__main__":
    main()
