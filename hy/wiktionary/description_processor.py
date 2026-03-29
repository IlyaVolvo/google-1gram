#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-13

description_processor.py

Builds final dictionary entry descriptions from POS description regions
extracted from a Wiktionary element.

Public interface:
process_description(text: str) -> str

Expected input:
'text' is the raw POS description region (string) for a single POS
section. Pre-processors (pp1/pp2/pp3) have already:
- normalized etymology placement,
- ensured that POS description bullets use '# ' when appropriate,
- handled -hy-բաց- structural issues.

Behavior:
- Process the POS region line by line.
- Bullet-items are lines whose first non-space character is '#'.
- FILTER_STRING (CSV) and DESC_TRIGGER (CSV) are applied to
  **cleaned** bullet lines using whole-word matching:
  * Cleaning: remove leading '#', remove '{{...}}', replace '[[foo]]'
    with 'foo', strip whitespace.
  * Lines whose cleaned text is empty are discarded before filtering.
  * Tokenization: split the cleaned line on whitespace and LATIN
    punctuation characters. Only exact token matches count.
  * FILTER_STRING: drop any bullet-item whose cleaned text has at least
    one token equal to any filter word.
  * DESC_TRIGGER: iterate the remaining bullet-items in order and
    drop each bullet-item that has any token equal to a trigger
    word, until you reach (1) a bullet-item with no trigger word,
    or (2) the last bullet-item. The first bullet-item that
    survives this process (or the last one, if all contain a
    trigger word) is selected.
- The selected bullet-item's cleaned text is returned.
- If USE_COUNT is False, return just the cleaned bullet text.
- If USE_COUNT is True:
  * Normalize COUNT_VALUE (string or int).
  * If COUNT_VALUE is empty/zero, return "fr=; ".
  * Otherwise, return "fr=<COUNT_VALUE>; <cleaned text>".
"""

import re
from typing import List, Optional, Union, Tuple

import debug_utils

# Globals configured by page_element_processor at runtime.
FILTER_STRING: Optional[str] = None  # e.g. "սերթ,քայլ" or None
DESC_TRIGGER: Optional[str] = None   # e.g. "ունտու" or CSV list
USE_COUNT: bool = False
COUNT_VALUE: Optional[Union[str, int]] = None

# Latin punctuation for tokenization (Armenian and other letters are
# left intact as part of tokens).
WORD_SPLIT_RE = re.compile(
    r"[ \t\r\n\f\v!\"#$%&'()*+,\-./:;<=>?@\[\]^_`{|}~]+"
)

# Simple Armenian-range check for sanity (for optional future use).
ARMENIAN_RE = re.compile(r"[\u0531-\u058F]")

# ---------------------------------------------------------------------------
# Low-level cleaning
# ---------------------------------------------------------------------------


def remove_nested_double_braces(text: str) -> str:
    """
    Remove all segments enclosed in nested '{{...}}' pairs.

    Example:
    'foo {{bar {{baz}} qux}} zip' -> 'foo zip'
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


def clean_bullet_text(line: str) -> str:
    """
    Clean a single bullet-item line:

    - Remove the leading '#' (after any indentation).
    - Remove nested '{{...}}'.
    - Replace '[[foo]]' with 'foo'.
    - Strip surrounding whitespace.
    """
    if not line:
        return ""

    stripped = line.lstrip()
    if not stripped.startswith("#"):
        return ""

    # Remove the leading '#' from the stripped line
    bullet = stripped[1:]

    bullet = remove_nested_double_braces(bullet)

    # Replace '[[foo]]' and '[[foo|bar]]' with visible text
    def _link_repl(m: re.Match) -> str:
        target = m.group(1)
        label = m.group(2)
        return label or target

    bullet = re.sub(r"\[\[([^|\]]+)(?:\|([^]]+))?\]\]", _link_repl, bullet)

    bullet = bullet.strip(" \t\r\n,;:()[]")
    return bullet.strip()


# ---------------------------------------------------------------------------
# Helpers for FILTER_STRING and DESC_TRIGGER
# ---------------------------------------------------------------------------


def parse_csv_values(value: Optional[str]) -> List[str]:
    """Split a comma-separated string into a list of non-empty trimmed tokens."""
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def tokenize_words(line: str) -> List[str]:
    """
    Tokenize a line into 'whole words' by splitting on whitespace and
    LATIN punctuation characters. This keeps Armenian and other script
    letters intact inside tokens.

    Example:
    '[[հինգ]]ից' -> ['[[հինգ]]ից']
    """
    if not line:
        return []
    tokens = WORD_SPLIT_RE.split(line)
    return [t for t in tokens if t]


def line_contains_any_word(line: str, words: List[str]) -> bool:
    """
    Return True if any of the given words matches a **whole token** in
    the line, after tokenization by WORD_SPLIT_RE.
    """
    if not words or not line:
        return False

    tokens = tokenize_words(line)
    if not tokens:
        return False

    token_set = set(tokens)
    return any(w in token_set for w in words)


def collect_bullet_lines(text: str) -> List[str]:
    """
    Collect bullet-item lines from the POS region.

    A bullet-item is any line whose first non-space character is '#'.
    Empty lines and non-bullet lines are ignored.
    """
    lines = text.splitlines()
    bullets: List[str] = []
    for line in lines:
        debug_utils._debug_log("description_processor: line", line)
        stripped = line.lstrip()
        if stripped.startswith("#"):
            bullets.append(line)
    debug_utils._debug_log(
        "description_processor: bullet_lines", "\n".join(bullets)
    )
    return bullets


# NEW: clean all bullets first, discard those that become empty
def build_cleaned_candidates(
    bullet_lines: List[str],
) -> List[Tuple[str, str]]:
    """
    From raw bullet lines, build a list of (raw_line, cleaned_text)
    pairs, discarding bullets whose cleaned_text is empty.

    This enforces the spec that bullet-items becoming empty after
    template processing are removed before filtering.
    """
    candidates: List[Tuple[str, str]] = []
    for raw in bullet_lines:
        cleaned = clean_bullet_text(raw)
        debug_utils._debug_log(
            "description_processor: cleaned_candidate",
            f"{raw}\n--> {cleaned}",
        )
        if not cleaned:
            continue
        candidates.append((raw, cleaned))
    return candidates


def select_bullet_line(
    candidates: List[Tuple[str, str]]
) -> Optional[Tuple[str, str]]:
    """
    Apply FILTER_STRING and DESC_TRIGGER logic to choose a bullet line,
    using whole-word matching on the **cleaned** text.

    candidates: list of (raw_line, cleaned_text) pairs.

    FILTER_STRING:
    Drop any candidate where the cleaned_text has at least one filter
    word equal to a token.

    DESC_TRIGGER:
    Let T be the list of trigger words.
    Iterate through the remaining candidates in order:
    - If a candidate has any token equal to a trigger word and it is
      not the last candidate, drop it and continue.
    - Otherwise (no trigger word in its tokens, or last candidate),
      select this candidate and stop.
    If all candidates contain trigger words, the last one is selected.
    """
    if not candidates:
        return None

    filter_words = parse_csv_values(FILTER_STRING)
    trigger_words = parse_csv_values(DESC_TRIGGER)

    # Apply FILTER_STRING on cleaned text
    filtered: List[Tuple[str, str]] = []
    for raw, cleaned in candidates:
        debug_utils._debug_log("description_processor: filter", raw)
        if line_contains_any_word(cleaned, filter_words):
            continue
        debug_utils._debug_log("description_processor: append", raw)
        filtered.append((raw, cleaned))

    if not filtered:
        return None

    # Apply DESC_TRIGGER logic on cleaned text
    n = len(filtered)
    for idx, (raw, cleaned) in enumerate(filtered):
        is_last = (idx == n - 1)
        debug_utils._debug_log(
            "description_processor: trigger_words", raw
        )
        if line_contains_any_word(cleaned, trigger_words) and not is_last:
            continue
        return (raw, cleaned)

    return filtered[-1] if filtered else None


# ---------------------------------------------------------------------------
# Description builder
# ---------------------------------------------------------------------------


def build_description(text: str) -> str:
    """
    Build the final description string from raw POS-region text.

    Algorithm:
    1. Extract all bullet-item lines (starting with '#').
    2. Clean each bullet line, discarding those whose cleaned text
       is empty.
    3. Apply FILTER_STRING and DESC_TRIGGER on the cleaned text to
       select one candidate.
    4. If the cleaned text is empty, return "".
    5. If USE_COUNT is False, return the cleaned text.
    6. If USE_COUNT is True, prefix with 'fr=<COUNT_VALUE>; '.
    """
    bullet_lines = collect_bullet_lines(text)

    # NEW: clean all bullets first and drop empty-after-cleaning ones
    candidates = build_cleaned_candidates(bullet_lines)
    if not candidates:
        debug_utils._debug_log("description_processor: chosen_line", "")
        debug_utils._debug_log("description_processor: cleaned", "")
        return ""

    chosen = select_bullet_line(candidates)
    raw_line, cleaned = chosen if chosen else ("", "")
    debug_utils._debug_log(
        "description_processor: chosen_line", raw_line or ""
    )
    debug_utils._debug_log("description_processor: cleaned", cleaned)

    if not cleaned:
        return ""

    if not USE_COUNT:
        return cleaned

    # USE_COUNT is True: normalize COUNT_VALUE.
    if COUNT_VALUE is None:
        freq = ""
    elif isinstance(COUNT_VALUE, str):
        freq = COUNT_VALUE.strip()
    else:
        try:
            ival = int(COUNT_VALUE)
            freq = "" if ival == 0 else str(ival)
        except Exception:
            freq = ""

    return f"fr={freq}; {cleaned}".strip()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def process_description(text: str) -> str:
    """
    Public entry point: text -> description.

    'text' is the raw POS description region for a single POS section.
    Returns the final cleaned description string or "".
    """
    debug_utils._debug_log("description_processor: input", text)
    return build_description(text)