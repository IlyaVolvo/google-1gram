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

They do not take POS maps or other parameters explicitly; shared data is
available via module-level globals if needed. The caller (page_element_processor)
chooses which pre-processors to execute via --nopp.

The purpose of pre-processing is to reshape irregular <text> layouts into
forms that match the expected structure (text-element-structure.txt), so that
the downstream text_processor and description_processor can operate reliably.

Currently implemented:

- pp1:
    If the 'Ստուգաբանություն' (etymology) section appears after the first
    POS section, move the whole 'Ստուգաբանություն' section to just before
    the first POS section.

- pp2:
    For each POS description region:
      * The POS region is the text after a line containing {{-hy-<x>-}}
        until the next POS template, or until a line whose first non-space
        characters start with '==' or '* '.
      * If the region already contains at least one line whose first non-space
        character is '#', the region is left unchanged.
      * Otherwise, for each line in the region:
          - If the line contains at least one Armenian letter, and
          - It is not already wiki-structural ('==', '* ', '#'),
            then prepend '# ' to that line.

    This ensures that POS description regions always contain bullet-items
    of the form '# ...' before they are passed to the standard text processor
    and description processor.

- pp3:
    Structural handling of -hy-բաց- POS sections, performed at the
    pre-processing stage:

      * If a -hy-բաց- POS section appears along with at least one other
        POS section:

          - For each POS section, compute a temporary description by calling
            description_processor.process_description() on its region.

          - Among POS sections whose POS keys map via POS_MAP to mapped tags
            and have non-empty descriptions, choose the "best" POS section
            by POS_WEIGHTS.

          - Append all non-empty descriptions from -hy-բաց- sections to that
            best POS section's description region in the raw text, and then
            remove the -hy-բաց- POS blocks entirely from the text.

      * If a -hy-բաց- POS section is the only POS section present or no
        other mapped POS with non-empty description exists:

          - Rename its POS line {{-hy-բաց-}} to {{-hy-անվ-}} in the text,
            so that later processing treats it as -hy-անվ- and maps it
            according to pos-map.txt.

    This ensures that downstream scripts no longer see raw -hy-բաց- keys;
    they see either merged content under an existing POS or a renamed
    -hy-անվ- POS.
"""

import re
from typing import List, Tuple

import description_processor as dp  # to compute provisional descriptions
import text_processor as tp        # to reuse POS extraction helpers

POS_TPL_EXACT_RE = re.compile(r"\{\{(-hy-([^}-]+)-)\}\}")
ARMENIAN_LETTER_RE = re.compile(r"[\u0531-\u0556\u0561-\u0587]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_pos_lines(lines: List[str]) -> List[int]:
    """Return indices of lines that contain a strict POS template {{-hy-<x>-}}."""
    return [i for i, line in enumerate(lines) if POS_TPL_EXACT_RE.search(line)]


def extract_pos_regions(lines: List[str]) -> List[Tuple[int, int, str]]:
    """
    Identify POS description regions.

    Returns a list of (start_line, end_line, full_key), where:

      - full_key is the POS template key '-hy-<x>-' from {{-hy-<x>-}}.
      - The region [start_line:end_line] is the POS description region, defined as:
          * lines AFTER the POS template line and
          * BEFORE the next POS template line, or
          * BEFORE the first line whose first non-space characters begin
            with '==' or '* '.
    """
    regions = []
    pos_infos = []
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
            if stripped.startswith("==") or stripped.startswith("* "):
                end = j
                break
        regions.append((start, end, full_key))
    return regions


# ---------------------------------------------------------------------------
# Pre-processors
# ---------------------------------------------------------------------------


def pp1(text: str) -> str:
    """
    pp1: Move 'Ստուգաբանություն' section before the first POS section.

    If an etymology header (e.g. '== Ստուգաբանություն ==' with any header
    level) appears after the first POS template {{-hy-<x>-}}, then:
      - Remove the whole etymology section (header + following lines up to the
        next header of same or higher level).
      - Reinsert it just before the first POS template line.

    If the etymology header is already before the first POS, or if there is
    no POS or no etymology header, the text is returned unchanged.
    """
    lines = text.splitlines()
    if not lines:
        return text

    pos_lines = find_pos_lines(lines)
    if not pos_lines:
        return text

    first_pos_idx = pos_lines[0]

    etym_header_re = re.compile(r"^=+\s*Ստուգաբանություն\s*=+\s*$")
    etym_start = None
    etym_level = None

    for i, line in enumerate(lines):
        stripped = line.strip()
        if etym_header_re.match(stripped):
            etym_start = i
            etym_level = len(stripped) - len(stripped.lstrip("="))
            break

    if etym_start is None or etym_start < first_pos_idx:
        return text

    etym_end = len(lines)
    header_re = re.compile(r"^(=+).+?(=+)\s*$")
    for j in range(etym_start + 1, len(lines)):
        stripped = lines[j].strip()
        m = header_re.match(stripped)
        if m:
            level = len(m.group(1))
            if etym_level is not None and level <= etym_level:
                etym_end = j
                break

    etym_block = lines[etym_start:etym_end]
    remaining = lines[:etym_start] + lines[etym_end:]

    pos_lines2 = find_pos_lines(remaining)
    if not pos_lines2:
        return text
    insert_at = pos_lines2[0]
    new_lines = remaining[:insert_at] + etym_block + remaining[insert_at:]
    return "\n".join(new_lines)


def pp2(text: str) -> str:
    """
    pp2: Ensure POS description regions contain bullet-items.

    For each POS description region:
      - If the region already contains at least one line whose first non-space
        character is '#', leave the region unchanged.
      - Otherwise, for each line in the region:
          * If it contains at least one Armenian letter, and
          * It is not already a structural wikitext line (starting with '==',
            '* ', or '#'),
            then prepend '# ' to that line.

    This converts plain-text POS descriptions into '#'-bullets before
    text_processor and description_processor are invoked.
    """
    lines = text.splitlines()
    if not lines:
        return text

    regions = extract_pos_regions(lines)
    if not regions:
        return text

    new_lines = lines[:]

    for start, end, full_key in regions:
        region = new_lines[start:end]
        if not region:
            continue

        if any(line.lstrip().startswith("#") for line in region):
            continue

        rewritten = []
        for line in region:
            stripped = line.strip()
            if not stripped:
                rewritten.append(line)
                continue

            lstripped = line.lstrip()
            if lstripped.startswith("==") or lstripped.startswith("* ") or lstripped.startswith("#"):
                rewritten.append(line)
                continue

            if ARMENIAN_LETTER_RE.search(line):
                indent = line[: len(line) - len(lstripped)]
                rewritten.append(f"{indent}# {stripped}")
            else:
                rewritten.append(line)

        new_lines[start:end] = rewritten

    return "\n".join(new_lines)


def pp3(text: str) -> str:
    """
    pp3: Structural handling of -hy-բաց- POS sections at the text level.

    Steps:
      1. Use text_processor.extract_pos_sections() to find all POS sections.
      2. If no '-hy-բաց-' POS is present, return text unchanged.
      3. For each POS section, compute a provisional description by calling
         description_processor.process_description() on its section text.
      4. Partition sections into:
           - bac_sections: full_key == '-hy-բաց-'
           - other_sections: everything else
      5. If there exists at least one other mapped POS section with non-empty
         description:
           - Among those, choose the "best" POS section using POS_WEIGHTS
             (imported via text_processor.POS_WEIGHTS).
           - Append all non-empty bac_sections descriptions to that chosen
             section's text in the raw <text> (lines).
           - Remove the -hy-բաց- POS lines (the template and its description
             region) entirely from the text.
      6. Otherwise (no other mapped POS with non-empty description):
           - For each bac_section, rename its POS template line
             '{{-hy-բաց-}}' to '{{-hy-անվ-}}' in the text.
           - No content is moved; only the POS key changes so that subsequent
             processing treats it as -hy-անվ-.

    This pre-processing ensures that downstream text_processor and
    description_processor do not see raw -hy-բաց- POS keys.
    """
    # Reuse text_processor's section extraction logic
    sections = tp.extract_pos_sections(text)
    if not sections:
        return text

    has_bac = any(full_key == "-hy-բաց-" for (full_key, _, _) in sections)
    if not has_bac:
        return text

    lines = text.splitlines()
    n = len(lines)

    # Build region index: map (start, end, full_key) to provisional desc
    regions = extract_pos_regions(lines)  # (start, end, full_key)

    # Map region boundaries to provisional descriptions and mapped tags
    bac_regions = []
    other_regions = []

    for start, end, full_key in regions:
        sec_text = "\n".join(lines[start:end]) if start < end else ""
        desc = dp.process_description(sec_text) or ""

        if full_key == "-hy-բաց-":
            bac_regions.append((start, end, full_key, desc))
        else:
            # Determine if this POS key is mapped via POS_MAP and with which tag
            mapped_tag = tp.POS_MAP.get(full_key)
            mapped = mapped_tag is not None
            other_regions.append((start, end, full_key, mapped, mapped_tag, desc))

    if not bac_regions:
        return text

    # Check for other mapped POS with non-empty desc
    mapped_with_desc = [
        (start, end, full_key, mapped_tag, desc)
        for (start, end, full_key, mapped, mapped_tag, desc) in other_regions
        if mapped and desc
    ]

    new_lines = lines[:]

    if mapped_with_desc:
        # Choose best target POS by POS_WEIGHTS
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

            # Append bac descriptions (as plain text) at the end of target region
            extra_descs = [d for (_, _, _, d) in bac_regions if d]
            if extra_descs:
                # Append them as extra '# ...' lines at the end of the target region
                appended_lines = [f"# {d}" for d in extra_descs]
                insert_pos = target_end
                new_lines = (
                    new_lines[:insert_pos] + appended_lines + new_lines[insert_pos:]
                )

            # Remove all -hy-բաց- regions from text (template line + region)
            # We must remove from bottom to top to keep indices stable.
            to_remove = []
            for start, end, full_key, desc in bac_regions:
                # Find the template line just before region start
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

            return "\n".join(new_lines)

    # Otherwise: no other mapped POS with desc → rename -hy-բաց- → -hy-անվ-
    renamed_lines = new_lines[:]
    for idx, line in enumerate(renamed_lines):
        if "{{-hy-բաց-}}" in line:
            renamed_lines[idx] = line.replace("{{-hy-բաց-}}", "{{-hy-անվ-}}")

    return "\n".join(renamed_lines)


PRE_PROCESSORS = {
    "pp1": pp1,
    "pp2": pp2,
    "pp3": pp3,
}