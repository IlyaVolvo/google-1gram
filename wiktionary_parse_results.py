#!/usr/bin/env python3
"""
wiktionary_parse_results.py

Reads:
  <base>.jsonl  (or '-' for stdin)

Writes:
  <base>.csv    (or stdout when input is '-')

CSV output:
  - One row per (word, part_of_speech)
  - Tags are emitted as *separate CSV columns* after the first 3 columns:
      word,frequency,part_of_speech,<tag1>,<tag2>,<tag3>,...

Rules:
  - If a word has multiple PoS, emit multiple rows (one per PoS).
  - Do NOT add SINGULAR unless explicitly stated ("singular of"/"singular form of").
  - Russian combined case+number tags (explicit-only):
      * NOMINATIVE_SINGULAR
      * NON_NOMINATIVE_SINGULAR
      * NON_NOMINATIVE_PLURAL
    (No separate NON_NOMINATIVE tag anymore.)

Notes:
  - Single-threaded, pipeline-friendly.
"""

from __future__ import annotations

import csv
import html
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


# -------------------- Basic HTML stripping --------------------

_TAG_RE = re.compile(r"<[^>]+>")

def strip_html(s: Optional[str]) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    return _TAG_RE.sub("", s)


# -------------------- Existing tags --------------------

DERIVATIVE_RE = re.compile(
    r"""(?iu)
    \b
    (?:form|inflection|inflected|declension|conjugation|
       plural|singular|
       nominative|genitive|dative|accusative|instrumental|prepositional|locative|vocative|ablative|partitive|
       masculine|feminine|neuter|common|
       comparative|superlative|
       past|present|future|imperfect|perfect|pluperfect|
       participle|gerund|infinitive|
       subjunctive|imperative|conditional|
       first|second|third(?:-person)?|person)
    \b
    (?:\s+\b
        (?:form|inflection|inflected|
           plural|singular|
           nominative|genitive|dative|accusative|instrumental|prepositional|locative|vocative|ablative|partitive|
           masculine|feminine|neuter|common|
           comparative|superlative|
           past|present|future|imperfect|perfect|pluperfect|
           participle|gerund|infinitive|
           subjunctive|imperative|conditional|
           first|second|third(?:-person)?|person)
        \b
    )*
    \s+
    (?:of|de|del|della|di|da|du|des|dos|das|do|von|van)\s+
    """,
    re.VERBOSE,
)

def has_any_example(entries: List[Dict[str, Any]]) -> bool:
    for e in entries:
        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue
            ex = d.get("examples")
            if isinstance(ex, list) and len(ex) > 0:
                return True
            pex = d.get("parsedExamples")
            if isinstance(pex, list) and len(pex) > 0:
                return True
    return False

def derivative_coverage(entries: List[Dict[str, Any]]) -> Tuple[int, int]:
    hits = total = 0
    for e in entries:
        lang = (e.get("language") or "").strip()
        anchor = f"#{lang}".lower() if lang else ""
        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue
            raw = d.get("definition") or ""
            if not isinstance(raw, str) or not raw:
                continue
            total += 1
            if anchor and anchor in raw.lower() and DERIVATIVE_RE.search(strip_html(raw)):
                hits += 1
    return hits, total

def noun_number_tag(entries: List[Dict[str, Any]]) -> Optional[str]:
    """
    Return 'PLURAL' or 'SINGULAR' ONLY if explicitly stated in definitions.
    Otherwise return None.
    """
    for e in entries:
        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue
            txt = strip_html(d.get("definition") if isinstance(d.get("definition"), str) else "").lower()
            if "plural of" in txt or "plural form of" in txt:
                return "PLURAL"
            if "singular of" in txt or "singular form of" in txt:
                return "SINGULAR"
    return None


# -------------------- Tier 1 + Tier 2 additional tags --------------------

LANG_NAMES = [
    "latin", "greek", "french", "old french", "middle french",
    "german", "old english", "anglo-norman",
    "spanish", "italian", "portuguese",
    "arabic", "hebrew", "aramaic",
    "persian", "sanskrit",
    "russian", "ukrainian", "polish", "czech",
    "dutch", "swedish", "norwegian", "danish",
    "japanese", "chinese", "korean",
]

def _lang_tag(lang: str) -> str:
    return lang.upper().replace(" ", "_").replace("-", "_")

def _gather_text_blobs(entries: List[Dict[str, Any]]) -> List[str]:
    blobs: List[str] = []
    for e in entries:
        for k in ("etymology_text", "etymology", "description"):
            v = e.get(k)
            if isinstance(v, str) and v.strip():
                blobs.append(strip_html(v).lower())

        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue
            dv = d.get("definition")
            if isinstance(dv, str) and dv.strip():
                blobs.append(strip_html(dv).lower())
            for kk in ("gloss", "text"):
                vv = d.get(kk)
                if isinstance(vv, str) and vv.strip():
                    blobs.append(strip_html(vv).lower())

    return blobs

def detect_origin_and_cognate_tags(entries: List[Dict[str, Any]]) -> List[str]:
    blobs = _gather_text_blobs(entries)
    tags: set[str] = set()

    for b in blobs:
        if "borrowed from" in b or "loanword from" in b:
            tags.add("LOANWORD")
        if "named after" in b or "eponym" in b:
            tags.add("EPONYM")

        for lang in LANG_NAMES:
            L = _lang_tag(lang)
            if f"from {lang}" in b:
                tags.add(f"FROM_{L}")
            if f"via {lang}" in b:
                tags.add(f"VIA_{L}")
            if f"cognate with {lang}" in b or f"cognate to {lang}" in b:
                tags.add(f"COGNATE_{L}")

    return sorted(tags)

REGISTER_MAP = {
    "archaic": "ARCHAIC",
    "obsolete": "OBSOLETE",
    "slang": "SLANG",
    "informal": "INFORMAL",
    "rare": "RARE",
    "offensive": "OFFENSIVE",
    "dialectal": "DIALECTAL",
    "poetic": "POETIC",
}

def detect_register_tags(entries: List[Dict[str, Any]]) -> List[str]:
    tags: set[str] = set()

    for e in entries:
        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue

            labels = d.get("labels")
            if isinstance(labels, list):
                for lab in labels:
                    if isinstance(lab, str):
                        key = lab.strip().lower()
                        if key in REGISTER_MAP:
                            tags.add(REGISTER_MAP[key])

            txt = strip_html(d.get("definition") if isinstance(d.get("definition"), str) else "").lower()
            for k, v in REGISTER_MAP.items():
                if k in txt:
                    tags.add(v)

    return sorted(tags)

def detect_morphology_tags(entries: List[Dict[str, Any]]) -> List[str]:
    tags: set[str] = set()
    for e in entries:
        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue
            txt = strip_html(d.get("definition") if isinstance(d.get("definition"), str) else "").lower()

            if "compound of" in txt:
                tags.add("COMPOUND")
            if "prefix" in txt or "prefixed with" in txt:
                tags.add("PREFIXED")
            if "suffix" in txt or "suffixed with" in txt:
                tags.add("SUFFIXED")
    return sorted(tags)

def detect_foreign_script_tags(word: str, entries: List[Dict[str, Any]]) -> List[str]:
    """
    NON_NATIVE_SCRIPT removed per your preference.
    Only keep:
      - TRANSLITERATED (romanization field)
      - FOREIGN_TERM (italics in definition HTML)
    """
    tags: set[str] = set()

    for e in entries:
        rom = e.get("romanization")
        if isinstance(rom, str) and rom.strip():
            tags.add("TRANSLITERATED")

    for e in entries:
        for d in e.get("definitions", []) or []:
            if not isinstance(d, dict):
                continue
            raw = d.get("definition") if isinstance(d.get("definition"), str) else ""
            if "<i>" in raw or "</i>" in raw:
                tags.add("FOREIGN_TERM")

    return sorted(tags)


# -------------------- Russian morphology tags (from Wiktionary definition text) --------------------

_CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")

RUS_GENDER_TAGS = {
    "masculine": "MASC",
    "feminine": "FEM",
    "neuter": "NEUT",
    "common": "COMMON_GENDER",
}

_NON_NOM_CASE_WORDS = (
    "genitive",
    "dative",
    "accusative",
    "instrumental",
    "prepositional",
    "locative",
    "vocative",
)

def _is_cyrillic_word(word: str) -> bool:
    return bool(word and _CYRILLIC_RE.search(word))

def _definition_texts(entries: List[Dict[str, Any]]) -> List[str]:
    return [
        strip_html(d.get("definition")).lower()
        for e in entries
        for d in e.get("definitions", []) or []
        if isinstance(d, dict) and isinstance(d.get("definition"), str)
    ]

def detect_russian_grammatical_tags(word: str, pos: str, entries: List[Dict[str, Any]]) -> List[str]:
    """
    Extract high-value Russian morphology tags from Wiktionary definition text.

    Case+Number combined tags (explicit-only):
      - NOMINATIVE_SINGULAR if "nominative singular of" appears
      - NON_NOMINATIVE_SINGULAR if any non-nominative case is mentioned AND singular is explicit
      - NON_NOMINATIVE_PLURAL if any non-nominative case is mentioned AND plural is explicit
      - Otherwise (no case mentioned): SINGULAR / PLURAL only if explicit
    """
    if not _is_cyrillic_word(word):
        return []

    tags: set[str] = set()
    defs = _definition_texts(entries)

    # --- Case + Number (combined) ---
    saw_nom_sg = any("nominative singular of" in d for d in defs)

    saw_singular = any(("singular of" in d) or ("singular form of" in d) for d in defs)
    saw_plural = any(("plural of" in d) or ("plural form of" in d) for d in defs)

    saw_nonnom = any((" of" in d) and any(cw in d for cw in _NON_NOM_CASE_WORDS) for d in defs)

    if saw_nom_sg:
        tags.add("NOMINATIVE_SINGULAR")
    else:
        # If case explicitly non-nominative, emit combined tags when number is explicit.
        if saw_nonnom:
            if saw_singular:
                tags.add("NON_NOMINATIVE_SINGULAR")
            if saw_plural:
                tags.add("NON_NOMINATIVE_PLURAL")

            # If case is mentioned but number isn't, emit nothing (explicit-only).
        else:
            # No case mentioned; keep plain number tags if explicit.
            if saw_singular:
                tags.add("SINGULAR")
            if saw_plural:
                tags.add("PLURAL")

    # --- Gender (explicit only; mostly NOUN/ADJ/VERB past forms) ---
    for g_word, g_tag in RUS_GENDER_TAGS.items():
        if any(f"{g_word} " in d and " of" in d for d in defs):
            tags.add(g_tag)

    # --- Adjectives: degree / short form (explicit only) ---
    if pos == "ADJ":
        if any("comparative form of" in d or ("comparative" in d and " of" in d) for d in defs):
            tags.add("COMPARATIVE")
        if any("superlative form of" in d or ("superlative" in d and " of" in d) for d in defs):
            tags.add("SUPERLATIVE")
        if any("short form of" in d or "short adjective form of" in d for d in defs):
            tags.add("SHORT_FORM")

    # --- Adverbs: degree only (explicit only) ---
    if pos == "ADV":
        if any("comparative form of" in d or ("comparative" in d and " of" in d) for d in defs):
            tags.add("COMPARATIVE")
        if any("superlative form of" in d or ("superlative" in d and " of" in d) for d in defs):
            tags.add("SUPERLATIVE")

    # --- Verbs: infinitive vs derived forms, tense, participles, aspect (explicit only) ---
    if pos == "VERB":
        if any("infinitive of" in d for d in defs):
            tags.update(["INFINITIVE", "INFLECTED_FORM"])

        if any("past tense of" in d or "past form of" in d for d in defs):
            tags.update(["PAST", "INFLECTED_FORM"])
        if any("present tense of" in d or "present form of" in d for d in defs):
            tags.update(["PRESENT", "INFLECTED_FORM"])
        if any("future tense of" in d or "future form of" in d for d in defs):
            tags.update(["FUTURE", "INFLECTED_FORM"])

        if any("imperative of" in d or "imperative form of" in d for d in defs):
            tags.update(["IMPERATIVE", "INFLECTED_FORM"])

        if any("present active participle" in d for d in defs):
            tags.update(["PARTICIPLE", "PRES_PART", "INFLECTED_FORM"])
        if any("past active participle" in d for d in defs):
            tags.update(["PARTICIPLE", "PAST_PART", "INFLECTED_FORM"])
        if any("present passive participle" in d for d in defs):
            tags.update(["PARTICIPLE", "PRES_PASS_PART", "INFLECTED_FORM"])
        if any("past passive participle" in d for d in defs):
            tags.update(["PARTICIPLE", "PAST_PASS_PART", "INFLECTED_FORM"])

        if any("adverbial participle" in d or "gerund of" in d for d in defs):
            tags.update(["GERUND", "INFLECTED_FORM"])

        if any("perfective of" in d or "perfective form of" in d for d in defs):
            tags.add("PERFECTIVE")
        if any("imperfective of" in d or "imperfective form of" in d for d in defs):
            tags.add("IMPERFECTIVE")

    return sorted(tags)


# -------------------- PoS grouping --------------------

def group_by_pos(entries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        pos = (e.get("partOfSpeech") or "UNKNOWN")
        pos_u = pos.upper() if isinstance(pos, str) else "UNKNOWN"
        out.setdefault(pos_u, []).append(e)
    return out


# -------------------- Tag extraction per (word, PoS) --------------------

def extract_tags(word: str, pos: str, entries: List[Dict[str, Any]]) -> List[str]:
    tags: set[str] = set()

    if has_any_example(entries):
        tags.add("HAS_EXAMPLE")

    hits, total = derivative_coverage(entries)
    if total:
        if hits == total:
            tags.add("LIKELY_DERIVATIVE")
        elif hits:
            tags.add("MAYBE_DERIVATIVE")

    defs = [
        strip_html(d.get("definition")).lower()
        for e in entries
        for d in e.get("definitions", []) or []
        if isinstance(d, dict) and isinstance(d.get("definition"), str)
    ]

    def contains(x: str) -> bool:
        return any(x in t for t in defs)

    # NOUN number (explicit only)
    if pos == "NOUN":
        num = noun_number_tag(entries)
        if num:
            tags.add(num)

    # VERB inflection-ish (generic; explicit-only phrases)
    if pos == "VERB":
        if contains("past participle"):
            tags.update(["PAST_PART", "INFLECTED_FORM"])
        if contains("present participle"):
            tags.update(["PRES_PART", "INFLECTED_FORM"])
        if contains("past tense") or contains("preterite"):
            tags.update(["PAST", "INFLECTED_FORM"])
        if contains("imperative"):
            tags.update(["IMP", "INFLECTED_FORM"])
        if contains("third-person singular") or contains("3rd-person singular") or contains("3rd person singular"):
            tags.update(["3PS", "INFLECTED_FORM"])
        if contains("subjunctive"):
            tags.update(["SUBJ", "INFLECTED_FORM"])
        if contains("infinitive"):
            tags.add("INF")
        if contains("form of") or contains("inflection of") or contains("conjugation of") or contains("conjugated form of"):
            tags.update(["FORM_OF", "INFLECTED_FORM"])

    # Tier 1 + Tier 2 additions
    tags.update(detect_origin_and_cognate_tags(entries))
    tags.update(detect_register_tags(entries))
    tags.update(detect_morphology_tags(entries))
    tags.update(detect_foreign_script_tags(word, entries))

    # Russian combined grammar tags
    tags.update(detect_russian_grammatical_tags(word, pos, entries))

    return sorted(tags)


# -------------------- IO / Main --------------------

def replace_ext(path: str, new_ext: str) -> str:
    base, _ = os.path.splitext(path)
    return base + new_ext

def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: wiktionary_parse_results.py <file.jsonl | ->", file=sys.stderr)
        sys.exit(2)

    src = sys.argv[1]
    use_stdin = src == "-"

    if use_stdin:
        fin = sys.stdin
    else:
        if not os.path.exists(src):
            print(f"ERROR: input not found: {src}", file=sys.stderr)
            sys.exit(2)
        fin = open(src, "r", encoding="utf-8")

    fout = sys.stdout

    try:
        w = csv.writer(fout)

        for line_no, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception as e:
                print(f"WARNING line {line_no}: bad JSON ({e})", file=sys.stderr)
                continue

            word = (obj.get("word") or "").strip()
            if not word:
                continue

            try:
                freq = int(obj.get("frequency") or 0)
            except Exception:
                freq = 0

            entries = obj.get("entries")
            if not isinstance(entries, list):
                continue

            by_pos = group_by_pos(entries)
            for pos, pos_entries in sorted(by_pos.items()):
                tags = extract_tags(word, pos, pos_entries)
                w.writerow([word, freq, pos] + tags)

    finally:
        if not use_stdin:
            try:
                fin.close()
            except Exception:
                pass
            try:
                fout.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
