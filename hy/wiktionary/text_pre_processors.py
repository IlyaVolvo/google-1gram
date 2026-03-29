#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Design: Aram
Coding: Perplexity
Date: 2026-03-26

text_pre_processors.py

Pre-processing passes for Armenian Wiktionary <text> elements.

Each pre-processor is a pure function:
    text_out = ppN(text_in)

page_element_processor reads <page> elements, extracts <text> content,
optionally sets DEBUG_WORD based on --debug <word>, then calls these
pre-processors before handing text to text_processor and
description_processor.
"""

import debug_utils
import re
from typing import List, Tuple
import description_processor as dp
import text_processor as tp

# ---------------------------------------------------------------------------
# Debugging support
# ---------------------------------------------------------------------------

DEBUG_WORD: str = ""          # set externally from page_element_processor
DEBUG_FILE: str = "debug.txt"  # output file for debugging


def _debug_dump(label: str, text: str) -> None:
    """
    Append the given text to DEBUG_FILE, preceded by a label line.

    The caller (page_element_processor) should set DEBUG_WORD to a non-empty
    string when --debug <word> is used. We do not inspect the word here; we
    only log when DEBUG_WORD is non-empty so the caller can control when
    logging happens.
    """
    global DEBUG_WORD
    if not DEBUG_WORD:
        return

    try:
        with open(DEBUG_FILE, "a", encoding="utf-8") as f:
            f.write(f"===== {label} =====\n")
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
            f.write("===== END =====\n\n")
    except Exception:
        # Debugging must never break normal processing
        pass


# ---------------------------------------------------------------------------
# Shared regexes and helpers
# ---------------------------------------------------------------------------

POS_TPL_EXACT_RE = re.compile(r"\{\{(-hy-([^}-]+)-)\}\}")
ARMENIAN_LETTER_RE = re.compile(r"[\u0531-\u0556\u0561-\u0587]")


def find_pos_lines(lines: List[str]) -> List[int]:
    """Return indices of lines that contain a strict POS template {{-hy-<x>-}}."""
    return [i for i, line in enumerate(lines) if POS_TPL_EXACT_RE.search(line)]


def extract_pos_regions(lines: List[str]) -> List[Tuple[int, int, str]]:
    """
    Identify POS description regions.

    Returns a list of (start_line, end_line, full_key), where:

      - full_key is the POS template key '-hy-<x>-' from {{-hy-<x>-}}.
      - A POS description region is:

          * The lines AFTER the POS template line, and
          * BEFORE the next POS template line {{-hy-<y>-}}, or
          * BEFORE the next header line whose first non-space characters
            start with '==', or
          * BEFORE the end of the <text>.

    List markup lines (starting with '* ', '- ', '# ') are treated as
    content inside the POS region and do not terminate it.
    """
    regions: List[Tuple[int, int, str]] = []
    pos_infos: List[Tuple[int, str]] = []

    for idx, line in enumerate(lines):
        m = POS_TPL_EXACT_RE.search(line)
        if m:
            pos_infos.append((idx, m.group(1)))

    if not pos_infos:
        return regions

    n = len(lines)

    for pos_idx, (line_idx, full_key) in enumerate(pos_infos):
        start = line_idx + 1
        next_pos = pos_infos[pos_idx + 1][0] if pos_idx + 1 < len(pos_infos) else n
        end = next_pos
        for j in range(start, next_pos):
            stripped = lines[j].lstrip()
            if stripped.startswith("=="):
                end = j
                break
        regions.append((start, end, full_key))

    return regions


# ---------------------------------------------------------------------------
# Pre-processors
# ---------------------------------------------------------------------------


def pp1(text: str) -> str:
    """
    pp1: Normalize placement of the 'Ստուգաբանություն' etymology header.

    Given the <text> content:

      - If there is no POS template {{-hy-<pos>-}}, return text unchanged.

      - If there is at least one POS template and at least one header
        line that contains 'Ստուգաբանություն':

          * first_pos_idx = index of first POS template line.
          * first_etym_idx = index of first header line (starts with '=')
            that contains 'Ստուգաբանություն'.

          * If first_etym_idx < first_pos_idx, return text unchanged.

          * If first_pos_idx < first_etym_idx, then:

              1) Insert a canonical '==Ստուգաբանություն==' line
                 immediately before the first POS template line.

              2) Remove every other header line anywhere in the text that
                 contains 'Ստուգաբանություն'.

      - If there is a POS template but no header containing
        'Ստուգաբանություն', return text unchanged.
    """
    debug_utils._debug_log("pp1", text)

    lines = text.splitlines()
    if not lines:
        return text

    pos_indices = find_pos_lines(lines)
    if not pos_indices:
        return text
    first_pos_idx = pos_indices[0]

    etym_indices: List[int] = []
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("=") and "Ստուգաբանություն" in stripped:
            etym_indices.append(i)

    if not etym_indices:
        return text

    first_etym_idx = min(etym_indices)

    if first_etym_idx < first_pos_idx:
        return text

    new_lines = lines[:]

    new_lines.insert(first_pos_idx, "==Ստուգաբանություն==")

    to_remove = []
    for i, line in enumerate(new_lines):
        stripped = line.lstrip()
        if stripped.startswith("=") and "Ստուգաբանություն" in stripped:
            if i == first_pos_idx:
                continue
            to_remove.append(i)

    for i in reversed(to_remove):
        del new_lines[i]

    out = "\n".join(new_lines)

    return out

def pp2(text: str) -> str:
    """
    pp2: Normalize POS description regions to use '# ' bullets.

    For each POS description region (as defined by extract_pos_regions):

      1) Normalize single-character list markers to '# ':
         - Lines whose first non-space characters are '* ' or '- '
           are rewritten to '# ' followed by the same content.

      2) After this normalization, if the region contains at least one
         line whose first non-space character is '#', the region is left
         unchanged.

      3) Otherwise, for each remaining line in the region:

         - If it contains at least one Armenian letter, and
         - It is not already a structural line starting with '==' or '#',

         prepend '# ' to that line.
    """
    lines = text.splitlines()
    if not lines:
        return text

    regions = extract_pos_regions(lines)
    if not regions:
        return text

    new_lines = lines[:]

    for start, end, full_key in regions:
        if start >= end:
            continue

        # First pass: normalize '* ' and '- ' to '# '.
        for idx in range(start, end):
            line = new_lines[idx]
            lstripped = line.lstrip()
            indent_len = len(line) - len(lstripped)
            indent = line[:indent_len]

            if lstripped.startswith("* "):
                content = lstripped[2:]
                new_lines[idx] = f"{indent}# {content}"
            elif lstripped.startswith("- "):
                content = lstripped[2:]
                new_lines[idx] = f"{indent}# {content}"

        # Re-slice region after normalization.
        region = new_lines[start:end]

        # If region already has any '#'-lines, leave it as is.
        if any(line.lstrip().startswith("#") for line in region):
            continue

        # Otherwise add '# ' to Armenian-bearing, non-structural lines.
        rewritten: List[str] = []
        for line in region:
            stripped = line.strip()
            if not stripped:
                rewritten.append(line)
                continue

            lstripped = line.lstrip()
            if lstripped.startswith("==") or lstripped.startswith("#"):
                rewritten.append(line)
                continue

            # Armenian-bearing lines become '# ' bullets.
            if ARMENIAN_LETTER_RE.search(line):
                indent_len = len(line) - len(lstripped)
                indent = line[:indent_len]
                rewritten.append(f"{indent}# {stripped}")
            else:
                rewritten.append(line)

        new_lines[start:end] = rewritten

    out = "\n".join(new_lines)
    return out

def pp3(text: str) -> str:
    """
    pp3: Structural handling of -hy-բաց- POS sections at the text level.

    See earlier explanations; this function optionally merges or retags
    -hy-բաց- sections based on description content.
    """
    debug_utils._debug_log("pp3", text)

    sections = tp.extract_pos_sections(text)
    if not sections:
        return text

    has_bac = any(full_key == "-hy-բաց-" for (full_key, _, _) in sections)
    if not has_bac:
        return text

    lines = text.splitlines()
    regions = extract_pos_regions(lines)

    bac_regions = []
    other_regions = []

    for start, end, full_key in regions:
        sec_text = "\n".join(lines[start:end]) if start < end else ""
        desc = dp.process_description(sec_text) or ""
        if full_key == "-hy-բաց-":
            bac_regions.append((start, end, full_key, desc))
        else:
            mapped_tag = tp.POS_MAP.get(full_key)
            mapped = mapped_tag is not None
            other_regions.append((start, end, full_key, mapped, mapped_tag, desc))

    if not bac_regions:
        return text

    new_lines = lines[:]

    mapped_with_desc = [
        (start, end, full_key, mapped_tag, desc)
        for (start, end, full_key, mapped, mapped_tag, desc) in other_regions
        if mapped and desc
    ]

    if mapped_with_desc:
        best = None
        best_w = -1
        for item in mapped_with_desc:
            start, end, full_key, mapped_tag, desc = item
            w = tp.POS_WEIGHTS.get(mapped_tag, 0)
            if w > best_w:
                best_w = w
                best = item

        if best is not None:
            target_start, target_end, target_key, target_tag, target_desc = best
            extra_descs = [d for (_, _, _, d) in bac_regions if d]
            if extra_descs:
                appended_lines = [f"# {d}" for d in extra_descs]
                insert_pos = target_end
                new_lines = new_lines[:insert_pos] + appended_lines + new_lines[insert_pos:]

        to_remove = []
        for start, end, full_key, desc in bac_regions:
            tpl_idx = start - 1
            if tpl_idx >= 0 and "-hy-բաց-" in new_lines[tpl_idx]:
                rm_start = tpl_idx
            else:
                rm_start = start
            rm_end = end
            to_remove.append((rm_start, rm_end))

        to_remove.sort(reverse=True)
        for rm_start, rm_end in to_remove:
            del new_lines[rm_start:rm_end]

        out = "\n".join(new_lines)
        _debug_dump("pp3 AFTER", out)
        return out

    renamed_lines = new_lines[:]
    for idx, line in enumerate(renamed_lines):
        if "{{-hy-բաց-}}" in line:
            renamed_lines[idx] = line.replace("{{-hy-բաց-}}", "{{-hy-անվ-}}")

    out = "\n".join(renamed_lines)

    return out


PRE_PROCESSORS = {
    "pp1": pp1,
    "pp2": pp2,
    "pp3": pp3,
}