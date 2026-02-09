#!/usr/bin/env python3
"""
wiktionary_validate_jsonl.py

Reads CSV input lines:  word,frequency
Queries: https://en.wiktionary.org/api/rest_v1/page/definition/{safe_word}

Outputs (file input mode):
  - <base>-VALIDATED.jsonl   (JSONL; one record per VALIDATED word)
  - R<base>.csv              (retry bucket: transient/network/429/5xx/parse errors, etc.)
  - N<base>.csv              (nonexistent bucket: HTTP 404)
  - <base>-REJECTED.csv      (HTTP 200 but locale missing/empty)

Outputs (stdin mode, input file "-"):
  - VALIDATED JSONL -> stdout
  - NO artifact files

PoS suffix behavior:
  - For querying / validated output word: strip trailing _<PoS> if present (years_NOUN -> years)
  - For artifact CSVs (R/N/REJECTED): preserve original token including PoS suffix

Flags:
  --max-validated : Stop when reaches that point, default to the end of the input file
  --cap        : try lowercase first; if it yields non-empty locale entries, also try capitalized and merge.
                If either attempt hits a transient error/exception, send to RETRY.
  --workers N  : number of threads (default 4)
  --debug      : verbose tracing to stderr (request URL, status, keys, classification)

Notes:
  - HTML is stripped from definitions[].definition values, but structure is preserved.
  - Safe URL encoding is always applied (avoids double-encoding).
  - Exponential backoff for 429/5xx and network errors.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
import time
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

USER_AGENT = "wordle-wiktionary-locale-dump/1.0 (https://example.invalid; contact: you@example.invalid)"

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json; charset=utf-8",
}

# -------------------- CLI --------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("lang", help="Locale key in REST JSON (e.g. en, de, fr, ru).")
    p.add_argument("file", help="Input CSV file (word,frequency) or '-' for stdin.")
    p.add_argument("--max-validated",type=int,default=0,help="Stop early once this many words have been VALIDATED (0 = no limit).",)
    p.add_argument("--cap", action="store_true", help="Try lowercase first, then capitalized; merge locale entries.")
    p.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 16).")
    p.add_argument("--debug", action="store_true", help="Enable verbose debug tracing to stderr.")
    return p.parse_args()


# -------------------- Debug --------------------

def dbg(enabled: bool, msg: str) -> None:
    if enabled:
        print(f"[DEBUG] {msg}", file=sys.stderr)


# -------------------- HTML stripping --------------------

class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._chunks: List[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._chunks.append(data)

    def get_text(self) -> str:
        return "".join(self._chunks)


def strip_html(html: str) -> str:
    if not html:
        return ""
    s = _HTMLStripper()
    s.feed(html)
    txt = s.get_text()
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


# -------------------- URL encoding (safe, no double-encode) --------------------

_PCT_ESC_RE = re.compile(r"%[0-9A-Fa-f]{2}")

def url_title(word: str) -> str:
    """
    Percent-encode Unicode/reserved chars for a URL path component.
    Avoid double-encoding: if every '%' is part of a valid %HH escape, assume already encoded.
    """
    if "%" in word:
        matches = list(_PCT_ESC_RE.finditer(word))
        if matches:
            covered = [False] * len(word)
            for m in matches:
                for i in range(m.start(), m.end()):
                    covered[i] = True
            if all(covered[i] or word[i] != "%" for i in range(len(word))):
                return word
    return quote(word, safe="")


# -------------------- PoS suffix stripping (ONLY for querying / validated output) --------------------

_POS_SUFFIXES = {
    "NOUN", "VERB", "ADJ", "ADV", "PROPN", "PRON", "DET", "ADP",
    "CONJ", "SCONJ", "CCONJ", "NUM", "PART", "INTJ", "AUX",
}

def strip_pos_suffix_for_query(token: str) -> str:
    if "_" not in token:
        return token
    base, suf = token.rsplit("_", 1)
    if base and suf.upper() in _POS_SUFFIXES:
        return base
    return token


# -------------------- REST fetching with backoff --------------------

BASE_URL = "https://en.wiktionary.org/api/rest_v1/page/definition/"
_RETRYABLE = {429, 500, 502, 503, 504}

def fetch_definition(
    query_word: str,
    session: requests.Session,
    debug_enabled: bool,
    max_attempts: int = 6
) -> Tuple[int, Optional[Dict[str, Any]]]:
    """
    Returns (status_code, json_or_none).
    Retries on transient errors (429/5xx) and network exceptions with exponential backoff.
    404 is returned immediately (nonexistent).
    Other non-200 returns immediately with json None.
    """
    # safe = url_title(query_word)
    safe = quote(query_word, safe="")  # Hebrew -> %D7%...
    url = BASE_URL + safe  # already encoded

    backoff = 0.5
    last_exc: Optional[str] = None

    for attempt in range(max_attempts):
        try:
            dbg(debug_enabled, f"REST GET {url} (attempt {attempt})")
            r = session.get(url, timeout=20, headers=DEFAULT_HEADERS)
            status = r.status_code
            dbg(debug_enabled, f"REST status {status} for word='{query_word}'")

            if status == 200:
                try:
                    data = r.json()
                    if isinstance(data, dict):
                        dbg(debug_enabled, f"JSON top-level keys: {sorted(list(data.keys()))[:40]}")
                    else:
                        dbg(debug_enabled, f"JSON type: {type(data)}")
                    return 200, data
                except Exception as e:
                    # JSON parse failure: treat as transient
                    last_exc = f"json parse error: {e}"
                    dbg(debug_enabled, f"JSON parse error; backoff {backoff:.2f}s; {last_exc}")
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 10.0)
                    continue

            if status == 404:
                return 404, None

            if status in _RETRYABLE:
                dbg(debug_enabled, f"Retryable status {status}; backoff {backoff:.2f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 10.0)
                continue

            # Non-retryable status
            dbg(debug_enabled, f"Non-retryable status {status} -> RETRY bucket")
            return status, None

        except requests.RequestException as e:
            last_exc = f"request exception: {e}"
            dbg(debug_enabled, f"RequestException; backoff {backoff:.2f}s; {last_exc}")
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 10.0)

    dbg(debug_enabled, f"Exhausted retries for word='{query_word}'. Last error: {last_exc or 'none'}")
    return 0, None


def get_locale_entries(data: Optional[Dict[str, Any]], lang: str) -> List[Dict[str, Any]]:
    if not data or not isinstance(data, dict):
        return []
    section = data.get(lang)
    if not section or not isinstance(section, list):
        return []
    return [x for x in section if isinstance(x, dict)]

def strip_html_in_list(xs):
    if not isinstance(xs, list):
        return xs
    out = []
    for x in xs:
        if isinstance(x, str):
            out.append(strip_html(x))
        elif isinstance(x, dict):
            out.append({k: (strip_html(v) if isinstance(v, str) else v) for k, v in x.items()})
        else:
            out.append(x)
    return out


def strip_html_in_parsed_examples(xs):
    """
    parsedExamples is usually: [{"example": "...html..."}, ...]
    Keep structure, strip HTML inside "example" string.
    """
    if not isinstance(xs, list):
        return xs
    out = []
    for x in xs:
        if isinstance(x, dict):
            x2 = dict(x)
            ex = x2.get("example")
            if isinstance(ex, str):
                x2["example"] = strip_html(ex)
            out.append(x2)
        elif isinstance(x, str):
            out.append(strip_html(x))
        else:
            out.append(x)
    return out


def strip_definitions(entries, debug_enabled: bool):
    cleaned = []
    for entry in entries:
        e2 = dict(entry)
        defs = entry.get("definitions")

        if isinstance(defs, list):
            new_defs = []
            for d in defs:
                if not isinstance(d, dict):
                    new_defs.append(d)
                    continue

                d2 = dict(d)

                # definition
                if isinstance(d2.get("definition"), str):
                    d2["definition"] = strip_html(d2["definition"])

                # examples (list[str])
                if isinstance(d2.get("examples"), list):
                    d2["examples"] = strip_html_in_list(d2["examples"])

                # parsedExamples (list[dict] with "example")
                if isinstance(d2.get("parsedExamples"), list):
                    d2["parsedExamples"] = strip_html_in_parsed_examples(d2["parsedExamples"])

                new_defs.append(d2)

            e2["definitions"] = new_defs

        cleaned.append(e2)
    return cleaned


# -------------------- IO helpers --------------------

def parse_input_stream(fin) -> List[Tuple[str, int]]:
    tasks: List[Tuple[str, int]] = []
    for raw in fin:
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        if low.startswith("word,") or low.startswith("term,"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            continue
        w = parts[0]
        try:
            f = int(parts[1])
        except Exception:
            continue
        if w:
            tasks.append((w, f))
    return tasks


def base_name_no_csv(path: str) -> Tuple[str, str]:
    d = os.path.dirname(os.path.abspath(path))
    b = os.path.basename(path)
    if b.lower().endswith(".csv"):
        b = b[:-4]
    return d, b


# -------------------- Outcomes --------------------

class Outcome:
    VALIDATED = "validated"
    REJECTED = "rejected"        # 200 but locale missing/empty
    NONEXISTENT = "nonexistent"  # 404
    RETRY = "retry"              # any transient/non-200/non-404/0


# -------------------- Core processing --------------------

def process_one(
    original_token: str,
    freq: int,
    lang: str,
    cap: bool,
    debug_enabled: bool,
) -> Tuple[str, str, str, int, List[Dict[str, Any]]]:
    """
    Returns (outcome, out_word, original_token, freq, entries)
      - out_word is PoS-stripped token used in validated JSONL and querying
      - original_token preserved for artifact CSVs
    """
    query_base = strip_pos_suffix_for_query(original_token)
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    def attempt(query_word: str) -> Tuple[int, List[Dict[str, Any]], bool]:
        status, data = fetch_definition(query_word, session=session, debug_enabled=debug_enabled)
        if status == 200 and isinstance(data, dict):
            loc = get_locale_entries(data, lang)
            loc2 = strip_definitions(loc, debug_enabled)
            dbg(debug_enabled, f"Locale '{lang}' entries: {len(loc2)} for query_word='{query_word}'")
            return 200, loc2, False
        if status == 404:
            dbg(debug_enabled, f"404 nonexistent for query_word='{query_word}'")
            return 404, [], False
        dbg(debug_enabled, f"Transient/non-200 status={status} for query_word='{query_word}' -> RETRY")
        return status, [], True

    dbg(debug_enabled, f"PROCESS token='{original_token}' freq={freq} query_base='{query_base}' cap={cap}")

    if not cap:
        status, entries, transient = attempt(query_base)
        if transient:
            return Outcome.RETRY, query_base, original_token, freq, []
        if status == 404:
            return Outcome.NONEXISTENT, query_base, original_token, freq, []
        if entries:
            return Outcome.VALIDATED, query_base, original_token, freq, entries
        return Outcome.REJECTED, query_base, original_token, freq, []

    # --cap:
    w_lower = query_base.lower()
    w_cap = query_base[:1].upper() + query_base[1:] if query_base else query_base

    status1, e1, transient1 = attempt(w_lower)
    if transient1:
        return Outcome.RETRY, query_base, original_token, freq, []

    if status1 == 404:
        status2, e2, transient2 = attempt(w_cap)
        if transient2:
            return Outcome.RETRY, query_base, original_token, freq, []
        if status2 == 404:
            return Outcome.NONEXISTENT, query_base, original_token, freq, []
        if e2:
            return Outcome.VALIDATED, query_base, original_token, freq, e2
        return Outcome.REJECTED, query_base, original_token, freq, []

    if not e1:
        status2, e2, transient2 = attempt(w_cap)
        if transient2:
            return Outcome.RETRY, query_base, original_token, freq, []
        if e2:
            return Outcome.VALIDATED, query_base, original_token, freq, e2
        return Outcome.REJECTED, query_base, original_token, freq, []

    status2, e2, transient2 = attempt(w_cap)
    if transient2:
        return Outcome.RETRY, query_base, original_token, freq, []

    merged = list(e1)
    if e2:
        merged.extend(e2)
    return Outcome.VALIDATED, query_base, original_token, freq, merged


# -------------------- Main --------------------

def main() -> None:
    args = parse_args()
    lang = args.lang.strip()
    if not lang:
        print("ERROR: lang must be non-empty (e.g. en, de, fr).", file=sys.stderr)
        sys.exit(2)

    # Read tasks
    if args.file == "-":
        tasks = parse_input_stream(sys.stdin)
        validated_out = sys.stdout
        r_out = n_out = rej_out = None
        out_paths = None
    else:
        in_path = args.file
        with open(in_path, "r", encoding="utf-8") as fin:
            tasks = parse_input_stream(fin)

        d, base = base_name_no_csv(in_path)
        validated_path = os.path.join(d, f"{base}-VALIDATED.jsonl")
        r_path = os.path.join(d, f"R{base}.csv")
        n_path = os.path.join(d, f"N{base}.csv")
        rej_path = os.path.join(d, f"{base}-REJECTED.csv")

        validated_out = open(validated_path, "w", encoding="utf-8")
        r_out = open(r_path, "w", encoding="utf-8")
        n_out = open(n_path, "w", encoding="utf-8")
        rej_out = open(rej_path, "w", encoding="utf-8")
        out_paths = (validated_path, r_path, n_path, rej_path)

    total = len(tasks)
    print(f"Processing {total} words. lang={lang} cap={args.cap} workers={args.workers} debug={args.debug}", file=sys.stderr)
    if out_paths:
        vp, rp, np, rjp = out_paths
        print(f"Validated: {vp}", file=sys.stderr)
        print(f"Retry:     {rp}", file=sys.stderr)
        print(f"Nonexist:  {np}", file=sys.stderr)
        print(f"Rejected:  {rjp}", file=sys.stderr)

    started = time.time()
    processed = validated = retry = nonexist = rejected = 0
    lastw: Optional[str] = None

    def worker(t: Tuple[str, int]) -> Tuple[str, str, str, int, List[Dict[str, Any]]]:
        w, f = t
        return process_one(w, f, lang=lang, cap=args.cap, debug_enabled=args.debug)

    try:
        with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futures = [ex.submit(worker, t) for t in tasks]

            for fut in cf.as_completed(futures):
                # If we already hit the limit, stop consuming results.
                if args.max_validated and validated >= args.max_validated:
                    # Cancel anything not started yet
                    for f in futures:
                        f.cancel()
                    break

                outcome, out_word, orig_token, freq, entries = fut.result()
                processed += 1
                lastw = orig_token

                dbg(args.debug, f"CLASSIFY token='{orig_token}' -> outcome={outcome} out_word='{out_word}'")

                if outcome == Outcome.VALIDATED:
                    # If this one would exceed the limit, stop (don’t write it).
                    if args.max_validated and validated >= args.max_validated:
                        for f in futures:
                            f.cancel()
                        break

                    validated += 1
                    obj = {"word": out_word, "frequency": freq, "entries": entries}
                    validated_out.write(json.dumps(obj, ensure_ascii=False) + "\n")

                elif outcome == Outcome.RETRY:
                    retry += 1
                    if r_out is not None:
                        r_out.write(f"{orig_token},{freq}\n")

                elif outcome == Outcome.NONEXISTENT:
                    nonexist += 1
                    if n_out is not None:
                        n_out.write(f"{orig_token},{freq}\n")

                else:  # REJECTED
                    rejected += 1
                    if rej_out is not None:
                        rej_out.write(f"{orig_token},{freq}\n")

                # progress report
                if processed == total or (processed % 100 == 0 and processed > 0):
                    elapsed = time.time() - started
                    avg_ms = (elapsed * 1000.0 / processed) if processed else 0.0

                    print(
                        f"\rProcessed:{processed}/{total}  "
                        f"Validated:{validated}  Retry:{retry}  "
                        f"Nonexist:{nonexist}  Rejected:{rejected}  "
                        f"Elapsed:{elapsed:.1f}s  Avg:{avg_ms:.1f}ms/word  "
                        f"Last:{lastw or ''}        ",
                        end="",
                        flush=True,
                        file=sys.stderr,
                    )


    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
    finally:
        if args.file != "-":
            try:
                validated_out.close()
            except Exception:
                pass
            for fh in (r_out, n_out, rej_out):
                try:
                    if fh is not None:
                        fh.close()
                except Exception:
                    pass

    print("\nDone.", file=sys.stderr)
    if lastw:
        print(f"Last word processed: {lastw}", file=sys.stderr)


if __name__ == "__main__":
    main()
