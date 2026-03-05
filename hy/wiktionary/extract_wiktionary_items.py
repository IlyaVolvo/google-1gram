#!/usr/bin/env python3
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

extract_wiktionary_items wiktionary.xml

Extract Armenian Wiktionary entries (word, POS tag, definition) from a dump
and write them to CSV.

Features:
- Detect POS from Armenian POS templates inside the etymology (or whole text).
- Map hy-Wiktionary POS templates to coarse POS tags (ADJ, ADV, NOUN, etc.).
- Filter words by length using a normalized form where 'և' is replaced by 'եւ'.
- Optionally skip words listed in a plain text file (comma-separated list):
  * Titles matching any skip word are excluded entirely.
  * Definition bullet items (lines starting with '#') that contain any skip
    word are removed from the output definition string.
- Optionally filter out records using --filter:
  * --filter takes a comma-separated list of words.
  * If any of these words occurs as a whole word in the final definition
    string of an entry, that entire entry is skipped.
- Show progress as the number of records written.

Examples:
  # Default length range 4–9, write to stdout
  ./extract_wiktionary_items wiktionary.xml > out.csv

  # Limit to words of normalized length 5–8
  ./extract_wiktionary_items --min 5 --max 8 wiktionary.xml > out.csv

  # Skip words listed in skip.txt (e.g. "արագ,զուգահեռաջիղ")
  ./extract_wiktionary_items --skip skip.txt wiktionary.xml > out.csv

  # Filter out entries whose definitions contain whole words "գիրք" or "տուն"
  ./extract_wiktionary_items --filter "գիրք,տուն" wiktionary.xml > out.csv
"""

import sys
import re
import csv
import argparse
import xml.etree.ElementTree as ET

# Exact POS mapping from your specification
POS_MAP = {
    "{{-hy-ած-}}": "ADJ",
    "{{-hy-մակ-}}": "ADV",
    "{{-hy-գո-}}": "NOUN",
    "{{-hy-բայ-}}": "VERB",
    "{{-hy-դեր-}}": "PRON",
    "{{-hy-թվ-}}": "NUM",
    "{{-hy-կապ-}}": "ADP",
    "{{-hy-շաղ-}}": "CONJ",
    "{{-hy-ձա-}}": "INTJ",
    "{{-hy-եղբ-}}": "MOD",
}

ETYM_HEADER_RE = re.compile(r"^==\s*Ստուգաբանություն\s*==\s*$", re.MULTILINE)
H2_HEADER_RE = re.compile(r"^==\s*[^=]+==\s*$", re.MULTILINE)
POS_TPL_RE = re.compile(r"(\{\{-hy-[^}]+\}\})")
DEF_RE = re.compile(r"^#\s*.+")  # keep leading '# '


def normalize_for_length(word: str) -> str:
    """Replace 'և' with 'եւ' before measuring length."""
    return word.replace("և", "եւ")


def get_relevant_section(text):
    """Return POS/definition region; prefer '== Ստուգաբանություն ==' if present."""
    m_ety = ETYM_HEADER_RE.search(text)
    if not m_ety:
        return text
    start_idx = m_ety.end()
    rest = text[start_idx:]
    m_next = H2_HEADER_RE.search(rest)
    if m_next:
        return rest[:m_next.start()]
    return rest


def extract_from_page(title_el, text_el, skip_set):
    if title_el is None or text_el is None or not text_el.text:
        return []

    word = (title_el.text or "").strip()
    text = text_el.text
    section = get_relevant_section(text)

    pos_hits = []
    for m in POS_TPL_RE.finditer(section):
        tpl = m.group(1)
        if tpl in POS_MAP:
            pos_hits.append((m.start(), m.end(), tpl))

    if not pos_hits:
        return []

    records = []
    for i, (start, end, tpl) in enumerate(pos_hits):
        block_start = end
        block_end = pos_hits[i + 1][0] if i + 1 < len(pos_hits) else len(section)
        block = section[block_start:block_end]

        defs = []
        for line in block.splitlines():
            line = line.rstrip()
            if DEF_RE.match(line):
                # Remove bullet if any skip word appears in it
                if any(sw in line for sw in skip_set):
                    continue
                defs.append(line)

        if not defs:
            continue

        definition_str = " ".join(defs)
        tag = POS_MAP[tpl]
        records.append((word, tag, definition_str))

    return records


def extract_wiktionary_items(xml_path, skip_set):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    m = re.match(r"\{.*\}", root.tag)
    ns = m.group(0) if m else ""
    page_tag = ns + "page"
    title_tag = ns + "title"
    text_tag = ns + "text"

    for page in root.iter(page_tag):
        title_el = page.find("./" + title_tag)
        text_el = page.find(".//" + text_tag)
        for rec in extract_from_page(title_el, text_el, skip_set):
            yield rec


def load_skip_file(path: str):
    """
    Load a comma-separated list of words from a text file into a set.
    All commas across the file are treated as separators.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError:
        sys.stderr.write(f"Warning: could not read skip file '{path}', ignoring.\n")
        return set()

    skip_set = set()
    for w in content.split(","):
        w = w.strip()
        if w:
            skip_set.add(w)
    return skip_set


def parse_filter_words(filter_arg: str):
    """Parse comma-separated filter words into a list and compile regex for whole-word matching."""
    if not filter_arg.strip():
        return [], None
    words = [w.strip() for w in filter_arg.split(",") if w.strip()]
    if not words:
        return [], None
    # Whole "word" in this context: use \b; should work reasonably with Armenian letters.
    pattern = r"\b(" + "|".join(re.escape(w) for w in words) + r")\b"
    return words, re.compile(pattern)


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
            "Comma-separated list of words. If any of these occurs as a whole "
            "word in the definition of an entry, that entry is skipped."
        ),
    )
    p.add_argument("xml_path", help="Path to Armenian Wiktionary XML dump.")
    return p.parse_args()


def main():
    args = parse_args()

    # Build skip set from file, if provided
    skip_set = set()
    if args.skip.strip():
        skip_set = load_skip_file(args.skip.strip())

    # Build filter word list and regex
    filter_words, filter_re = parse_filter_words(args.filter)

    writer = csv.writer(sys.stdout)
    writer.writerow(["word", "tag", "definition"])

    count = 0
    for word, tag, definition in extract_wiktionary_items(args.xml_path, skip_set):
        # Skip titles that are in skip list
        if word in skip_set:
            continue

        # Length filter
        norm_word = normalize_for_length(word)
        if not (args.min <= len(norm_word) <= args.max):
            continue

        # Definition filter by whole filter-words
        if filter_re is not None and filter_re.search(definition):
            continue

        writer.writerow([word, tag, definition])
        count += 1

        if count % 100 == 0:
            sys.stderr.write(f"\rRecorded {count} words...")
            sys.stderr.flush()

    sys.stderr.write(f"\rRecorded {count} words.\n")
    sys.stderr.flush()


if __name__ == "__main__":
    main()
