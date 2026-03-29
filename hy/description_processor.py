#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-11 (updated)

description_utils.py

Utilities for building dictionary entry descriptions from a single
definition string (may contain multiple '#' bullets).

Main entry point:
    build_description(
        definition: str,
        count: str | int | None = None,
        filter_string: str | None = None,
        trigger_second_bullet: str | None = None,
    ) -> str

Arguments:
- definition: raw definition text for a single word (possibly multiple bullets).
- count: frequency / word count; when not None and not "0"/0, we prefix "fr=<count>; ".
- filter_string: comma-separated values; bullets containing any of these
  substrings are dropped entirely before bullet selection (e.g. "հնց,ժղ,...").
- trigger_second_bullet: comma-separated values; if the first bullet contains
  any of these triggers, the second bullet is preferred.

If, after filtering and cleaning, there is no non-empty bullet, the function
returns an empty string and the caller should skip that entry.
"""

import re
from typing import Iterable, List, Optional, Union


def remove_nested_double_braces(text: str) -> str:
    """Remove all segments enclosed in nested '{{...}}'."""
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
    """Split a definition string into bullet segments starting with '#'."""
    if not definition:
        return []

    positions = [i for i, ch in enumerate(definition) if ch == "#"]
    if not positions:
        # No bullets: treat whole definition as a single pseudo-bullet
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


def parse_csv_values(value: Optional[str]) -> List[str]:
    """Parse a comma-separated string into a list of non-empty trimmed tokens."""
    if not value:
        return []
    parts: List[str] = []
    for item in value.split(","):
        token = item.strip()
        if token:
            parts.append(token)
    return parts


def filter_bullets(
    bullets: Iterable[str],
    filter_words: Iterable[str],
) -> List[str]:
    """
    Remove bullets that contain any of the filter_words as substrings.
    Matching is case-sensitive and simple substring.
    """
    bullets = list(bullets)
    if not bullets:
        return []

    fw = list(filter_words)
    if not fw:
        return bullets

    kept: List[str] = []
    for b in bullets:
        if any(w in b for w in fw):
            continue
        kept.append(b)
    return kept


def choose_bullet(
    definition: str,
    filter_string: Optional[str] = None,
    trigger_second_bullet: Optional[str] = None,
) -> str:
    """
    Choose and clean the appropriate bullet:

    - Split the definition into bullets starting with '#'.
    - Drop bullets containing any filter_string word.
    - If no bullets remain, return "".
    - If only one bullet remains, use that one.
    - If at least two bullets remain and the first contains any trigger word
      (from trigger_second_bullet), use the second; otherwise use the first.
    """
    bullets = split_bullets(definition)
    if not bullets:
        return ""

    filter_words = parse_csv_values(filter_string)
    bullets = filter_bullets(bullets, filter_words)
    if not bullets:
        return ""

    if len(bullets) == 1:
        return clean_bullet_text(bullets[0])

    triggers = parse_csv_values(trigger_second_bullet)
    first = bullets[0]
    second = bullets[1]

    if triggers and any(t in first for t in triggers):
        return clean_bullet_text(second)

    return clean_bullet_text(first)


def build_description(
    definition: str,
    count: Optional[Union[str, int]] = None,
    filter_string: Optional[str] = None,
    trigger_second_bullet: Optional[str] = None,
) -> str:
    """
    Build the final description string from raw definition text and parameters.

    - First pick and clean a bullet via choose_bullet (applying filter_string
      and trigger_second_bullet).
    - If the cleaned bullet is empty, return "".
    - If count is None or 0/"0", return just the cleaned bullet.
    - Otherwise return "fr=<count>; <cleaned>".
    """
    cleaned = choose_bullet(
        definition,
        filter_string=filter_string,
        trigger_second_bullet=trigger_second_bullet,
    )
    if not cleaned:
        return ""

    if count is None:
        return cleaned

    # Normalize count to string, but treat numeric/str "0" as no frequency prefix
    try:
        if isinstance(count, str):
            c_str = count.strip()
            if c_str == "" or c_str == "0":
                return cleaned
            freq = c_str
        else:
            if int(count) == 0:
                return cleaned
            freq = str(count)
    except Exception:
        return cleaned

    return f"fr={freq}; {cleaned}"
