#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

extract_wiktionary_items wiktionary.xml

From a Wiktionary XML dump the script extracts Wiktionary entries: word in the
<title> element, along with part-of-speech (POS) tag, and word definition and
writes them into CSV file.

Usage:
    extract_wiktionary_items.py [options] wiktionary.xml > out.csv

Output CSV:
    word, tag, definition

Features (per specification):
1. Process only <title>-s that are strings of Armenian letters (after stripping
   invisible characters like ZERO WIDTH NO-BREAK SPACE).
   Skip and count separately titles whose pages:
   a) have non-Armenian characters in <title>;
   b) contain text '#REDIRECT' (case-insensitive) in <text>;
   c) have titles outside the length range after normalization, where
      normalization is done by replacing 'և' with 'եւ'.

2. Detect Armenian POS strings: templates of the form {{-hy-<x>-}} in the
   <text> element inside <page>. These templates start POS sections.
   There may be several POS sections. Each POS section is defined as:

   - POS start: the line containing {{-hy-<x>-}}.
   - POS description region: all lines *after* that line, up to (but not
     including) the first line whose first non-space characters start with
     any of:
         "=="   (a heading),
         "* "   (a list item at top level).
   - The boundary line itself (starting with "{{", "==", or "* ") is not part
     of the description region and is where the search for the next POS
     section continues.

3. For each POS section’s description region:

   - If there is at least one line whose first non-space character is '#',
     treat all such lines as definition bullets.
   - If there are no '#' bullets at all, then for each line in the description
     region, if it contains at least one Armenian letter and does not already
     start with '#', synthesize a bullet by prepending "# " in front of that
     line. These synthetic bullets are used as the definition lines.

   - Concatenate all bullet lines (real and/or synthetic) into a single
     definition string.
   - Pass that string to description_utils.build_description() along with:
       filter_string = value of --filter (comma-separated string) or None
       trigger_second_bullet = value of --desc-trigger or None
       count = None (no frequency in this script).
   - build_description returns a description string or empty string.

   a) Create a separate record in the main CSV for each POS section whose
      Armenian POS template {{-hy-<x>-}} maps to a typical POS via pos-map.txt
      AND whose build_description result is non-empty.
      Mapping rule: in the template {{-hy-<x>-}}, extract <x>; then look for
      a key "-hy-<x>-" in pos-map.txt. The value on that line is the typical
      POS tag for this section.

   b) Example behavior:
      - For xorovats.xml (խորոված), the script should produce three records:
          խորոված, VERB, խորովել բայի հարակատար դերբայը
          խորոված, NOUN, նորբ. անբոց կրակի վրա շամփուրով եփած միս
          խորոված, ADJ, խորովելով եփած
      - For the ուրբաթ page, the description region after {{-hy-գո-}} contains
        only the line "շաբաթվա հինգերորդ օրն է, հինգշաբթիի և շաբաթի միջև"
        before the heading "===== Հոմանիշներ =====", so the record is:
          ուրբաթ, NOUN, շաբաթվա հինգերորդ օրն է, հինգշաբթիի և շաբաթի միջև

   c) No output record is created for a POS section if:
      i) there is no mapping for its {{-hy-<x>-}} in pos-map.txt;
     ii) build_description() returns no description (empty string).

4. Skipped/filtered records (the ones NOT written to the main CSV) go to
   filtered-extract.csv with columns:
       word, tag, filter, definition

   "filter" column values:
   i)  'skipped'        for skip-file words (titles in skip list).
   ii) 'REDIRECT'       for words whose <page> contains a redirect; tag and
                        definition are blank.
   iii) a tag string    when we found at least one POS template {{-hy-<x>-}}
        but none of its <x> values match any entry in pos-map.txt; in that
        case we record the first unmapped key "-hy-<x>-" as both tag and filter.
        definition is blank.
   iv) 'no tag'         if no {{-hy-<x>-}} templates are found for that <title>.
        In these rows, tag and definition are blank.
   v)  'no desc'        if we have at least one mappable POS section (i.e. the
        template maps via pos-map.txt) but build_description() returns no
        description for that section. In these rows, tag is the mapped POS
        (e.g. VERB) and definition is blank.

   Note:
   - Pages skipped because of non-Armenian titles or out-of-range length are
     not written to filtered-extract.csv; they are only counted.

5. Progress reporting:
   - Show progress as the number of input words (<title> elements) processed.

6. At the end, print:
   a) total titles processed - <count>
   b) extracted entries - <count>
   c) skipped/filtered entries - <count>
   d) titles with non-Armenian characters - <count>
   e) titles ignored because of redirects - <count>

7. Single-output mode:
   - Optional argument --single <v>, where <v> is 'y' or 'n' (default 'n').
   - When --single n: behavior as above, a separate CSV record is created
     for every POS section that produces a description.
   - When --single y: at most one CSV record is created per word. Among all
     POS sections that produce a description, the one with the highest POS
     weight is selected:
        NOUN - 12, ADJ - 11, VERB - 10, ADV - 9, PRON - 8,
        NUM  - 7,  ADP - 6,  CONJ - 5, MOD - 4, INTJ - 3,
        ABBR - 2,  PLC - 1.
     Only that (word, tag, definition) is written to the main CSV; other
     POS sections for the same word are ignored for output and not recorded
     as filtered (the word is considered successfully extracted).
"""

import sys
import re
import csv
import argparse
import os
import xml.etree.ElementTree as ET

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PARENT_DIR)

from description_processor import build_description  # type: ignore

POS_TPL_EXACT_RE = re.compile(r"\{\{(-hy-([^}-]+)-)\}\}")  # {{-hy-<x>-}}
ARMENIAN_TITLE_RE = re.compile(r"^[\u0531-\u0556\u0561-\u0587]+$")
REDIRECT_RE = re.compile(r"#REDIRECT", re.IGNORECASE)
ARMENIAN_LETTER_RE = re.compile(r"[\u0531-\u0556\u0561-\u0587]")

POS_WEIGHTS = {
    "NOUN": 12,
    "ADJ": 11,
    "VERB": 10,
    "ADV": 9,
    "PRON": 8,
    "NUM": 7,
    "ADP": 6,
    "CONJ": 5,
    "MOD": 4,
    "INTJ": 3,
    "ABBR": 2,
    "PLC": 1,
}


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


def load_pos_map(path: str) -> dict:
    pos_map = {}
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

def extract_pos_sections(text: str):
    """
    Yield POS sections as tuples: (full_key, x, section_text).

    Implementation strictly follows:

    - Find every line that contains a POS template {{-hy-<x>-}}.
    - For each such line at index i:
        * Look at lines i+1, i+2, ... until a boundary line is found.
        * Boundary line: first non-space characters start with
              "{{"  or "==" or "* ".
          The boundary line itself is NOT part of this POS section.
        * section_text is the concatenation of all lines from i+1 up to
          (but not including) the boundary line, or up to end of text.
    - The next POS search continues from the boundary line (or end of text).
    """

    sections = []
    lines = text.splitlines(keepends=False)

    # Precompute all POS template matches per line
    pos_lines = []  # list of (line_index, full_key, x)
    for idx, line in enumerate(lines):
        m = POS_TPL_EXACT_RE.search(line)
        if m:
            full_key = m.group(1)       # e.g. "-hy-գո-"
            x = m.group(2) or ""        # e.g. "գո"
            pos_lines.append((idx, full_key, x))

    if not pos_lines:
        return sections

    n = len(lines)

    for pos_idx, (line_idx, full_key, x) in enumerate(pos_lines):
        # Start looking at the NEXT line after the POS template
        start_line = line_idx + 1

        # Default end line is either the line before the next POS template,
        # or the last line of the text
        if pos_idx + 1 < len(pos_lines):
            next_pos_line_idx = pos_lines[pos_idx + 1][0]
        else:
            next_pos_line_idx = n

        end_line = next_pos_line_idx

        # Now refine end_line based on boundary rules
        for j in range(start_line, next_pos_line_idx):
            stripped = lines[j].lstrip()
            if stripped.startswith("==") or stripped.startswith("* "):
                end_line = j
                break

        # Join lines from start_line to end_line (non-inclusive)
        if start_line < end_line:
            section_text = "\n".join(lines[start_line:end_line])
        else:
            section_text = ""

        sections.append((full_key, x, section_text))

    return sections

def collect_bullets(section_text: str) -> str:
    lines = section_text.splitlines()
    bullet_lines = []
    has_real_bullets = False

    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("#"):
            has_real_bullets = True
            bullet_lines.append(stripped)

    if has_real_bullets:
        return " ".join(bullet_lines)

    synthetic = []
    for line in lines:
        if ARMENIAN_LETTER_RE.search(line):
            text = line.strip()
            if text:
                synthetic.append("# " + text)

    return " ".join(synthetic)


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
        help="Path to text file with comma-separated words to skip."
    )
    p.add_argument(
        "--filter",
        type=str,
        default="",
        help="Comma-separated values passed as filter_string to build_description."
    )
    p.add_argument(
        "--desc-trigger",
        type=str,
        default="",
        help="Comma-separated values passed as trigger_second_bullet to build_description."
    )
    p.add_argument(
        "--single",
        type=str,
        default="n",
        help="If 'y', output at most one record per word, choosing the POS "
             "with highest weight; otherwise output all POS records (default: n).",
    )
    p.add_argument("xml_path", help="Path to Armenian Wiktionary XML dump.")
    return p.parse_args()


def main():
    args = parse_args()
    single_mode = (args.single.lower() == "y")

    pos_map_path = os.path.join(SCRIPT_DIR, "pos-map.txt")
    pos_map = load_pos_map(pos_map_path)

    skip_set = load_skip_file(args.skip.strip()) if args.skip.strip() else set()

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

        sections = extract_pos_sections(text)

        if not sections:
            filtered_writer.writerow([title, "", "no tag", ""])
            filtered_entries += 1
            continue

        candidates = []
        first_unmapped_key = None

        for full_key, x_val, sec_text in sections:
            mapped_tag = pos_map.get(full_key)
            raw_defs = collect_bullets(sec_text)

            if not mapped_tag:
                if first_unmapped_key is None:
                    first_unmapped_key = full_key
                continue

            if not raw_defs:
                filtered_writer.writerow([title, mapped_tag, "no desc", ""])
                filtered_entries += 1
                continue

            desc = build_description(
                raw_defs,
                count=None,
                filter_string=args.filter if args.filter.strip() else None,
                trigger_second_bullet=args.desc_trigger if args.desc_trigger.strip() else None,
            )

            if not desc:
                filtered_writer.writerow([title, mapped_tag, "no desc", ""])
                filtered_entries += 1
                continue

            candidates.append((mapped_tag, desc))

        if not candidates:
            if first_unmapped_key is not None:
                filtered_writer.writerow(
                    [title, first_unmapped_key, first_unmapped_key, ""]
                )
                filtered_entries += 1
            continue

        if single_mode:
            best_tag, best_desc, best_weight = None, None, -1
            for tag, desc in candidates:
                w = POS_WEIGHTS.get(tag, 0)
                if w > best_weight:
                    best_weight = w
                    best_tag, best_desc = tag, desc
            if best_tag is not None and best_desc is not None:
                out_writer.writerow([title, best_tag, best_desc])
                extracted_entries += 1
        else:
            for tag, desc in candidates:
                out_writer.writerow([title, tag, desc])
                extracted_entries += 1

    sys.stderr.write(f"\rProcessed titles: {total_titles}\n")
    sys.stderr.write(f"Extracted entries - {extracted_entries}\n")
    sys.stderr.write(f"Skipped/filtered entries - {filtered_entries}\n")
    sys.stderr.write(f"Titles with non-Armenian characters - {non_armenian_titles}\n")
    sys.stderr.write(f"Titles ignored because of redirects - {redirect_titles}\n")
    sys.stderr.flush()
    filtered_file.close()


if __name__ == "__main__":
    main()