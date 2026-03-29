#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-11

description_processor.py

Builds final dictionary entry descriptions from POS description regions
extracted from a Wiktionary <text> element.

Public interface:
    process_description(text: str) -> str

Expected input:
    'text' is the raw POS description region (string) for a single POS
    section. It may contain one or more bullet items starting with '#', or
    plain lines that have already been normalized into bullets by pre-
    processing (pp2).

Behavior:
  - Interpret bullet items as segments starting with '#'.
  - Optionally drop bullets containing any of the --filter terms.
  - Clean each bullet:
      * remove leading '#';
      * remove nested '{{...}}';
      * remove 'տե՛ս';
      * replace '[[foo]]' with 'foo';
      * trim whitespace.
  - Skip bullets whose cleaned text is empty.
  - Choose a cleaned bullet as description:
      * if there is only one candidate: use it;
      * if there are at least two candidates and any of the--desc-trigger
        words occurs in the first RAW bullet, use the cleaned text of the
        second candidate; otherwise use the cleaned text of the first.
  - If no cleaned candidate bullet remains, return "".
  - If USE_COUNT is True and COUNT_VALUE is non-zero, prefix the result
    with 'fr=<COUNT_VALUE>; '. In this script, count-related globals can
    be left unused; page_element_processor sets USE_COUNT=False by default.
"""

import re
from typing import List, Optional, Tuple, Union

# Globals configured by page_element_processor
FILTER_STRING: Optional[str] = None
DESC_TRIGGER: Optional[str] = None
USE_COUNT: bool = False
COUNT_VALUE: Optional[Union[str, int]] = None

ARMENIAN_LETTER_RE = re.compile(r"[\u0531-\u0556\u0561-\u0587]")


# ---------------------------------------------------------------------------
# Low-level cleaning
# ---------------------------------------------------------------------------


def remove_nested_double_braces(text: str) -> str:
    """
    Remove all segments enclosed in nested '{{...}}' pairs.

    Example:
        'foo {{bar {{baz}} qux}} zip' -> 'foo  zip'
    """
    result: List[str] = []
    depth = 0
    i = 0
    n = len(text)

    while i < n:
        if i + 1 < n and text[i] == "{" and text[i + 1] == "{":
            depth += 1
            i += 2
            continue
        if i + 1 < n and text[i] == "}" and text[i + 1] == "}":
            if depth > 0:
                depth -= 1
            i += 2
            continue
        if depth == 0:
            result.append(text[i])
        i += 1

    return "".join(result)


def split_bullets(definition: str) -> List[str]:
    """
    Split definition text into bullet segments starting with '#'.

    If no '#' is present, treat the entire definition as a single pseudo-bullet.
    """
    if not definition:
        return []

    positions = [i for i, ch in enumerate(definition) if ch == "#"]
    if not positions:
        return [definition]

    bullets: List[str] = []
    for idx, start in enumerate(positions):
        end = positions[idx + 1] if idx + 1 < len(positions) else len(definition)
        bullets.append(definition[start:end])
    return bullets


def clean_bullet_text(bullet: str) -> str:
    """
    Clean a single bullet string:
      - Remove leading '#'.
      - Remove nested '{{...}}'.
      - Remove 'տե՛ս'.
      - Replace '[[foo]]' with 'foo'.
      - Strip whitespace.
    """
    if not bullet:
        return ""

    if bullet.startswith("#"):
        bullet = bullet[1:]

    bullet = remove_nested_double_braces(bullet)
    bullet = bullet.replace("տե՛ս", "")
    bullet = re.sub(r"\[\[([^|\]]+)\]\]", r"\1", bullet)
    return bullet.strip()


# ---------------------------------------------------------------------------
# High-level selection
# ---------------------------------------------------------------------------


def parse_csv_values(value: Optional[str]) -> List[str]:
    """Split a comma-separated string into a list of non-empty trimmed tokens."""
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def filter_bullets(bullets: List[str], filter_words: List[str]) -> List[str]:
    """
    Drop bullets that contain any of the filter_words as substrings.

    Matching is case-sensitive and uses simple substring search.
    """
    if not bullets:
        return []
    if not filter_words:
        return bullets

    kept: List[str] = []
    for b in bullets:
        if any(w in b for w in filter_words):
            continue
        kept.append(b)
    return kept


def collect_candidates(definition: str) -> List[Tuple[str, str]]:
    """
    Convert raw definition text into candidate (raw, cleaned) bullet pairs:

      1. Split into raw bullets.
      2. Apply FILTER_STRING (drop bullets containing filter words).
      3. Clean each remaining bullet.
      4. Keep only bullets whose cleaned text is non-empty.

    Returns:
        List[(raw_bullet, cleaned_bullet)].
    """
    bullets = split_bullets(definition)
    if not bullets:
        return []

    fw = parse_csv_values(FILTER_STRING)
    bullets = filter_bullets(bullets, fw)

    candidates: List[Tuple[str, str]] = []
    for b in bullets:
        cleaned = clean_bullet_text(b)
        if cleaned:
            candidates.append((b, cleaned))
    return candidates


def choose_cleaned_bullet(definition: str) -> str:
    """
    Choose a cleaned bullet according to trigger rules:

      - Build a list of (raw, cleaned) candidates with non-empty cleaned text.
      - If none, return "".
      - If only one candidate, return its cleaned text.
      - If there are at least two candidates and any DESC_TRIGGER word occurs
        in the first raw bullet, return the cleaned text of the second
        candidate; otherwise return the cleaned text of the first.
    """
    candidates = collect_candidates(definition)
    if not candidates:
        return ""

    if len(candidates) == 1:
        return candidates[0][1]

    raw1, clean1 = candidates[0]
    _, clean2 = candidates[1]

    triggers = parse_csv_values(DESC_TRIGGER)
    if triggers and any(t in raw1 for t in triggers):
        return clean2

    return clean1


def build_description(definition: str) -> str:
    """
    Build the final description string from raw definition text.

    - Choose a cleaned bullet via choose_cleaned_bullet().
    - If empty, return "".
    - If USE_COUNT is False or COUNT_VALUE is zero/empty, return just the
      cleaned bullet.
    - Otherwise, return 'fr=<COUNT_VALUE>; <cleaned>'.
    """
    cleaned = choose_cleaned_bullet(definition)
    if not cleaned:
        return ""

    if not USE_COUNT:
        return cleaned

    if COUNT_VALUE is None:
        return cleaned

    try:
        if isinstance(COUNT_VALUE, str):
            c_str = COUNT_VALUE.strip()
            if not c_str or c_str == "0":
                return cleaned
            freq = c_str
        else:
            if int(COUNT_VALUE) == 0:
                return cleaned
            freq = str(COUNT_VALUE)
    except Exception:
        return cleaned

    return f"fr={freq}; {cleaned}"


def process_description(text: str) -> str:
    """
    Public entry point: text -> description.

    'text' is the raw POS description region for a single POS section.
    Returns the final cleaned description string or "".
    """
    return build_description(text)