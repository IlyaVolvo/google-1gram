#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-03

text_processor.py

Standard processing of a normalized <text> element that conforms as much
as possible to the structure in text-element-structure.txt.

Responsibilities:
  - Identify POS sections via templates {{-hy-<x>-}}.
  - Determine POS description regions for each POS section.
  - Map Armenian POS templates (-hy-<x>-) to "typical" POS tags via POS_MAP.
  - Pass POS description regions to description_processor.process_description()
    to obtain the final definition text for each POS section.
  - Provide enough detail for page_element_processor to decide whether
    failures are due to:
       * no POS templates ('no tag'),
       * unmapped POS keys,
       * mapped POS with empty descriptions ('no desc').

pp1 and pp2 from text_pre_processors.py are responsible for moving the
etymology section and ensuring that POS description regions contain '#'
bullets when appropriate. pp3 is responsible for restructuring -hy-բաց-
POS sections.

This module assumes pre-processing has already been applied to the text.
"""

import re
from typing import Dict, List, Tuple

from description_processor import process_description

# These globals are assigned from page_element_processor at runtime.
POS_MAP: Dict[str, str] = {}
POS_WEIGHTS: Dict[str, int] = {}

POS_TPL_EXACT_RE = re.compile(r"\{\{(-hy-([^}-]+)-)\}\}")


# ---------------------------------------------------------------------------
# POS section extraction
# ---------------------------------------------------------------------------


def extract_pos_sections(text: str) -> List[Tuple[str, str, str]]:
    """
    Extract POS sections from <text>.

    Returns:
        List of (full_key, x, section_text), where:
          - full_key is the POS template key '-hy-<x>-' from {{-hy-<x>-}}.
          - x is the <x> part.
          - section_text is the POS description region:
              * lines after the POS template line, up to
              * the next POS template line, or
              * the first line whose first non-space characters begin with
                '==' or '* '.
    """
    lines = text.splitlines(keepends=False)
    n = len(lines)

    pos_infos: List[Tuple[int, str, str]] = []
    for idx, line in enumerate(lines):
        m = POS_TPL_EXACT_RE.search(line)
        if m:
            full_key = m.group(1)
            x = m.group(2) or ""
            pos_infos.append((idx, full_key, x))

    if not pos_infos:
        return []

    sections: List[Tuple[str, str, str]] = []

    for i, (line_idx, full_key, x) in enumerate(pos_infos):
        start_line = line_idx + 1
        next_pos_line_idx = pos_infos[i + 1][0] if i + 1 < len(pos_infos) else n
        end_line = next_pos_line_idx

        for j in range(start_line, next_pos_line_idx):
            stripped = lines[j].lstrip()
            if stripped.startswith("==") or stripped.startswith("* "):
                end_line = j
                break

        section_text = "\n".join(lines[start_line:end_line]) if start_line < end_line else ""
        sections.append((full_key, x, section_text))

    return sections


def extract_pos_keys(text: str) -> List[str]:
    """
    Helper: return all full POS keys '-hy-<x>-' found in the text, in order.

    Used by page_element_processor to distinguish 'no tag' from other failure
    modes (e.g. POS templates present but no mapped POS).
    """
    keys: List[str] = []
    for m in POS_TPL_EXACT_RE.finditer(text):
        keys.append(m.group(1))
    return keys


# ---------------------------------------------------------------------------
# POS description extraction (detailed + simple)
# ---------------------------------------------------------------------------


def extract_pos_descriptions_with_details(
    text: str,
) -> List[Tuple[str, bool, str, str]]:
    """
    Extract POS descriptions with detailed classification.

    Returns a list of tuples per POS section:

        (full_key, mapped, mapped_tag, desc)

    where:
      - full_key: POS template key '-hy-<x>-'.
      - mapped: True if full_key is present in POS_MAP.
      - mapped_tag: the mapped POS tag (e.g. NOUN, ADJ) if mapped,
                    or full_key itself if unmapped (for easier reporting).
      - desc: the description string returned by process_description(), or ""
              if no description was produced for this section.

    This function does not apply single-output selection; that is done by
    page_element_processor via select_single_by_weight().
    """
    sections = extract_pos_sections(text)
    if not sections:
        return []

    details: List[Tuple[str, bool, str, str]] = []

    for full_key, x, sec_text in sections:
        mapped_tag = POS_MAP.get(full_key)
        mapped = mapped_tag is not None

        desc = ""
        if mapped:
            desc = process_description(sec_text) or ""

        if not mapped:
            mapped_tag = full_key  # for reporting unmapped keys

        details.append((full_key, mapped, mapped_tag, desc))

    return details


def extract_pos_descriptions(text: str) -> List[Tuple[str, str, str]]:
    """
    Compatibility wrapper: return (full_key, mapped_tag, desc) for POS sections
    that are mapped and have a non-empty description.
    """
    details = extract_pos_descriptions_with_details(text)
    result: List[Tuple[str, str, str]] = []
    for full_key, mapped, mapped_tag, desc in details:
        if mapped and desc:
            result.append((full_key, mapped_tag, desc))
    return result


# ---------------------------------------------------------------------------
# Single-output selection
# ---------------------------------------------------------------------------


def select_single_by_weight(
    candidates: List[Tuple[str, str, str]]
) -> List[Tuple[str, str, str]]:
    """
    Select at most one candidate (full_key, mapped_tag, description) by POS_WEIGHTS.

    candidates:
        List[(full_key, mapped_tag, description)].

    Returns:
        Single-element list containing the best candidate, or [] if none.

    The best candidate is the one whose mapped_tag has the highest weight
    according to POS_WEIGHTS. Tags not present in POS_WEIGHTS are treated
    as weight 0.
    """
    best = None
    best_weight = -1

    for full_key, tag, desc in candidates:
        w = POS_WEIGHTS.get(tag, 0)
        if w > best_weight:
            best_weight = w
            best = (full_key, tag, desc)

    return [best] if best else []