#!/usr/bin/env python3

"""
Validate words against Wiktionary REST API and emit CSV artifacts.

Input CSV format (assumed):
  term,term_frequency

Validated output CSV lines:
  <word>,<frequency>,<PoS1>,...,<PoSN>,<additional>

Additional tags (semicolon-separated, deduped):
  - NOUN: SINGULAR or PLURAL (best-effort)
  - VERB: best-effort INFLECTED_FORM markers inferred from definition text

Artifacts (file mode), for input foo.csv:
  foo-VALIDATED.csv   (validated words)
  Nfoo.csv            (non-existent: both probes 404 when --cap, or single 404 when no --cap)
  Rfoo.csv            (hard failures: 429/5xx/other http errors)
  foo-REJECTED.csv    (retry bucket: wrong locale OR ANY probe threw exception)

STDIN mode:
  file argument "-" reads from STDIN
  validated -> STDOUT
  others    -> STDERR (nonexistent, rejected, retry)
"""

import sys
import os
import re
import html
import time
import random
import socket
import argparse
import threading
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# --- GLOBAL SAFETY ---
socket.setdefaulttimeout(15)

# --- GLOBALS / STATE ---
stats_lock = threading.Lock()

validated_out_lines: List[str] = []
hard_rejected_lines: List[str] = []     # R<base>.csv
nonexistent_lines: List[str] = []       # N<base>.csv
retry_lines: List[str] = []             # <base>-REJECTED.csv (wrong locale OR exception)

validated_count = 0
processed_count = 0
nonexistent_count = 0
hard_rejected_count = 0
retry_count = 0
last_processed_word: Optional[str] = None

MAX_VALIDATED: Optional[int] = None          # --size / -s (validated output target)
MIN_FREQUENCY: Optional[int] = None          # --min-frequency / -f (stop reading input when below)
DEBUG = False

VALIDATED_OUTFILE: Optional[str] = None
NONEXISTENT_OUTFILE: Optional[str] = None
HARD_REJECTED_OUTFILE: Optional[str] = None
RETRY_OUTFILE: Optional[str] = None

DEFAULT_WORKERS = 8

# Rate-limit backoff controls
RATE_LIMIT_MAX_RETRIES = 6
RATE_LIMIT_BASE_DELAY = 0.75
RATE_LIMIT_MAX_DELAY = 20.0

# --- HTML stripping for Wiktionary definitions ---
_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = html.unescape(s)
    return _TAG_RE.sub("", s)


def normalize_term(term: str) -> str:
    """
    Strip trailing POS suffix like 'plus_ADV' -> 'plus' ONLY when suffix looks like POS tag.
    Keeps internal underscores intact.
    """
    term = (term or "").strip()
    if "_" not in term:
        return term
    base, suffix = term.rsplit("_", 1)
    if suffix.isalpha() and suffix.isupper() and 2 <= len(suffix) <= 6:
        return base
    return term


def ensure_csv_input_path(path: str) -> str:
    if path == "-":
        return path
    base = os.path.basename(path)
    _root, ext = os.path.splitext(base)
    if ext == "":
        return path + ".csv"
    if ext.lower() != ".csv":
        raise ValueError(f"Input must be a .csv file (or omit extension). Got: {path}")
    return path


def default_out_paths(input_csv_path: str, out_arg_validated: Optional[str] = None) -> Tuple[str, str, str, str]:
    """
    File mode naming for foo.csv:
      foo-VALIDATED.csv
      Nfoo.csv
      Rfoo.csv
      foo-REJECTED.csv   (retry bucket)
    """
    input_dir = os.path.dirname(os.path.abspath(input_csv_path))
    base = os.path.basename(input_csv_path)
    root, _ext = os.path.splitext(base)

    if out_arg_validated:
        if os.path.dirname(out_arg_validated):
            validated_path = out_arg_validated
        else:
            validated_path = os.path.join(input_dir, out_arg_validated)
    else:
        validated_path = os.path.join(input_dir, f"{root}-VALIDATED.csv")

    nonexistent_path = os.path.join(input_dir, f"N{root}.csv")
    hard_rejected_path = os.path.join(input_dir, f"R{root}.csv")
    retry_path = os.path.join(input_dir, f"{root}-REJECTED.csv")

    return validated_path, nonexistent_path, hard_rejected_path, retry_path


def reset_artifacts(input_csv_path: str) -> None:
    v, n, r, rt = default_out_paths(input_csv_path, out_arg_validated=None)
    for p in (v, n, r, rt):
        if os.path.exists(p):
            os.remove(p)


def extract_pos_list(lang_section: List[Dict[str, Any]]) -> List[str]:
    pos_list: List[str] = []
    seen = set()
    for entry in lang_section or []:
        pos = (entry.get("partOfSpeech") or "").strip()
        if not pos:
            continue
        pos_u = pos.upper()
        if pos_u not in seen:
            seen.add(pos_u)
            pos_list.append(pos_u)
    return pos_list


def is_plural_noun_entry(lang_section: List[Dict[str, Any]]) -> bool:
    plural_markers = (
        "plural of",
        "plural form of",
        "plurals of",
        "pluriel de",
        "plural von",
        "plural de",
        "forma plural",
    )
    for entry in lang_section or []:
        for d in entry.get("definitions", []) or []:
            text = strip_html(d.get("definition", "")).lower()
            if any(m in text for m in plural_markers):
                return True
    return False

# supposedely extract a tag that tells that the word was borrowed
def has_transclusion_markup(lang_section: List[Dict[str, Any]]) -> bool:
    """
    Detect MediaWiki transclusion markup in Wiktionary REST definition HTML.
    We check the raw HTML definition string (NOT stripped text), because stripping removes attributes.
    """
    for entry in lang_section or []:
        defs = entry.get("definitions", []) or []
        for d in defs:
            raw_html = d.get("definition") or ""
            if "mw:Transclusion" in raw_html:
                return True
    return False

def has_any_example(lang_section) -> bool:
    """
    Return True if any definition entry for the target language contains at least one example.
    Wiktionary REST usually provides examples in 'examples' (list[str]) and/or
    'parsedExamples' (list[dict] with key 'example').
    """
    for entry in (lang_section or []):
        for d in (entry.get("definitions") or []):
            ex = d.get("examples")
            if isinstance(ex, list) and len(ex) > 0:
                # examples are often strings; we treat presence as enough
                return True

            pex = d.get("parsedExamples")
            if isinstance(pex, list) and len(pex) > 0:
                # parsedExamples are often objects with {"example": "..."}
                # presence is enough; optionally require non-empty example text:
                for obj in pex:
                    if isinstance(obj, dict) and (obj.get("example") or "").strip():
                        return True
                # if they are not dicts, just presence is enough
                return True

    return False


def extract_additional_info(pos_list: List[str], lang_section: List[Dict[str, Any]]) -> str:
    tags = set()

    if has_any_example(lang_section):
        tags.add("HAS_EXAMPLE")

    # MediaWiki transclusion markup present in the definition HTML, seems to be useless
    #if has_transclusion_markup(lang_section):
    #    tags.add("TRANSCLUSION")

    def_texts: List[str] = []
    for entry in lang_section or []:
        for d_obj in entry.get("definitions", []) or []:
            t = strip_html(d_obj.get("definition") or "").lower()
            if t:
                def_texts.append(t)

    def contains_any(needles: List[str]) -> bool:
        for t in def_texts:
            for n in needles:
                if n in t:
                    return True
        return False

    is_noun = "NOUN" in pos_list
    is_verb = "VERB" in pos_list

    if is_noun:
        if is_plural_noun_entry(lang_section):
            tags.add("PLURAL")
        else:
            tags.add("SINGULAR")

    if is_verb:
        if contains_any(["past participle", "participe passé", "partizip ii"]):
            tags.update(["PAST_PART", "INFLECTED_FORM"])
        if contains_any(["present participle", "gerund", "participe présent", "partizip i"]):
            tags.update(["PRES_PART", "INFLECTED_FORM"])
        if contains_any(["past tense", "preterite", "prétérit", "imparfait"]):
            tags.update(["PAST", "INFLECTED_FORM"])
        if contains_any(["third-person singular", "3rd-person singular", "3rd person singular"]):
            tags.update(["3PS", "INFLECTED_FORM"])
        if contains_any(["imperative"]):
            tags.update(["IMP", "INFLECTED_FORM"])
        if contains_any(["infinitive"]):
            tags.add("INF")
        if contains_any(["subjunctive"]):
            tags.update(["SUBJ", "INFLECTED_FORM"])
        if contains_any(["form of", "inflection of", "conjugation of", "conjugated form of"]):
            tags.update(["FORM_OF", "INFLECTED_FORM"])

    return ";".join(sorted(tags))


def fetch_definition(word: str) -> Tuple[int, Optional[Dict[str, Any]], str]:
    """
    Returns (status_code, json_dict_or_none, url)
    status_code == -1 means exception in request/parse.
    Retries on 429 with exponential backoff.
    """
    safe_word = quote(word, safe="")
    url = f"https://en.wiktionary.org/api/rest_v1/page/definition/{safe_word}"
    headers = {"User-Agent": "WordValidatorBot/1.0 (local script)"}

    attempt = 0
    while True:
        try:
            resp = requests.get(url, headers=headers, timeout=(5, 10))

            if DEBUG:
                print(f"[DEBUG] REST {resp.status_code} {url} (attempt {attempt})", file=sys.stderr)

            if resp.status_code == 429:
                if attempt >= RATE_LIMIT_MAX_RETRIES:
                    return 429, None, url

                retry_after = resp.headers.get("Retry-After")
                delay: Optional[float] = None
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except Exception:
                        delay = None

                if delay is None:
                    delay = min(RATE_LIMIT_MAX_DELAY, RATE_LIMIT_BASE_DELAY * (2 ** attempt))
                    delay *= (0.8 + random.random() * 0.4)

                if DEBUG:
                    print(f"[DEBUG] 429 rate limited; sleeping {delay:.2f}s then retrying", file=sys.stderr)

                time.sleep(delay)
                attempt += 1
                continue

            if resp.status_code != 200:
                return resp.status_code, None, url

            data = resp.json()

            if DEBUG:
                if isinstance(data, dict):
                    print(f"[DEBUG] JSON keys: {list(data.keys())}", file=sys.stderr)

            if not isinstance(data, dict):
                return 200, None, url

            return 200, data, url

        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Exception fetching '{word}': {type(e).__name__}: {e}", file=sys.stderr)
            return -1, None, url


def _merge_ordered_unique(dst: List[str], src: List[str]) -> None:
    seen = set(dst)
    for x in src:
        if x not in seen:
            seen.add(x)
            dst.append(x)


def check_wiktionary(task_data: Tuple[str, int, str], lang_code: str, use_cap: bool) -> bool:
    """
    NEW behavior (your request):
      - If --cap: probe non-cap first AND cap second, then produce ONE output line.
      - If one probe failed with exception (status == -1): put input line into retry bucket (<base>-REJECTED.csv)
      - On success: output ONE validated line using the non-cap word (original normalized), with merged tags once.
    """
    global validated_count, processed_count, nonexistent_count, hard_rejected_count, retry_count, last_processed_word

    word_raw, freq, raw_line = task_data
    lower = word_raw
    cap = word_raw.capitalize()

    # Early stop if validated target reached
    with stats_lock:
        if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
            return False

    probes: List[Tuple[str, int, Optional[Dict[str, Any]], str]] = []

    # Always probe lower first
    s1, d1, u1 = fetch_definition(lower)
    probes.append((lower, s1, d1, u1))

    # If --cap, probe capitalized as well (second)
    if use_cap and cap != lower:
        s2, d2, u2 = fetch_definition(cap)
        probes.append((cap, s2, d2, u2))

    # If ANY probe threw exception => retry bucket (single line, once)
    if any(status == -1 for (_w, status, _d, _u) in probes):
        with stats_lock:
            retry_lines.append(f"{raw_line},exception\n")
            retry_count += 1
            processed_count += 1
            last_processed_word = word_raw
        return False

    # Collect successful (200) locale sections and merge PoS/tags
    merged_pos: List[str] = []
    merged_add_tags: List[str] = []
    any_valid_locale = False
    any_200_missing_locale = False

    for w, status, data, _url in probes:
        if status != 200 or not isinstance(data, dict) or not data:
            continue
        section = data.get(lang_code)
        if not (isinstance(section, list) and section):
            any_200_missing_locale = True
            continue

        any_valid_locale = True
        pos = extract_pos_list(section)
        add = extract_additional_info(pos, section)

        _merge_ordered_unique(merged_pos, pos)

        # merge additional tags (semicolon separated)
        if add:
            add_parts = [p for p in add.split(";") if p]
            _merge_ordered_unique(merged_add_tags, add_parts)

    # If we have at least one valid locale section: write ONE validated line (non-cap)
    if any_valid_locale:
        additional = ";".join(sorted(set(merged_add_tags)))
        validated_csv = ",".join([lower, str(freq)] + merged_pos + [additional]) + "\n"

        with stats_lock:
            if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                processed_count += 1
                last_processed_word = word_raw
                return False
            validated_out_lines.append(validated_csv)
            validated_count += 1
            processed_count += 1
            last_processed_word = word_raw
        return True

    # If any probe returned 200 but missing locale => retry bucket (wrong locale)
    if any_200_missing_locale:
        with stats_lock:
            retry_lines.append(f"{raw_line},wrong_locale\n")
            retry_count += 1
            processed_count += 1
            last_processed_word = word_raw
        return False

    # If all probes are 404 => NONEXISTENT
    if all(status == 404 for (_w, status, _d, _u) in probes):
        with stats_lock:
            nonexistent_lines.append(f"{raw_line}\n")
            nonexistent_count += 1
            processed_count += 1
            last_processed_word = word_raw
        return False

    # Otherwise: hard reject based on the "most important" non-404 status (prefer 429, then other)
    statuses = [status for (_w, status, _d, _u) in probes]
    if 429 in statuses:
        reason = "rate_limited"
    else:
        # first non-200/non-404 status
        bad = next((s for s in statuses if s not in (200, 404)), None)
        reason = f"http_{bad}" if bad is not None else "unknown_error"

    with stats_lock:
        hard_rejected_lines.append(f"{raw_line},{reason}\n")
        hard_rejected_count += 1
        processed_count += 1
        last_processed_word = word_raw
    return False


def flush_outputs() -> None:
    with stats_lock:
        if validated_out_lines:
            if VALIDATED_OUTFILE:
                with open(VALIDATED_OUTFILE, "a", encoding="utf-8") as v:
                    v.writelines(validated_out_lines)
            else:
                sys.stdout.writelines(validated_out_lines)
                sys.stdout.flush()
            validated_out_lines[:] = []

        if nonexistent_lines:
            if NONEXISTENT_OUTFILE:
                with open(NONEXISTENT_OUTFILE, "a", encoding="utf-8") as n:
                    n.writelines(nonexistent_lines)
            else:
                sys.stderr.writelines(nonexistent_lines)
                sys.stderr.flush()
            nonexistent_lines[:] = []

        if hard_rejected_lines:
            if HARD_REJECTED_OUTFILE:
                with open(HARD_REJECTED_OUTFILE, "a", encoding="utf-8") as r:
                    r.writelines(hard_rejected_lines)
            else:
                sys.stderr.writelines(hard_rejected_lines)
                sys.stderr.flush()
            hard_rejected_lines[:] = []

        if retry_lines:
            if RETRY_OUTFILE:
                with open(RETRY_OUTFILE, "a", encoding="utf-8") as rr:
                    rr.writelines(retry_lines)
            else:
                sys.stderr.writelines(retry_lines)
                sys.stderr.flush()
            retry_lines[:] = []


def parse_input_csv(path: str, min_len: Optional[int], max_len: Optional[int], use_stdin: bool) -> List[Tuple[str, int, str]]:
    tasks: List[Tuple[str, int, str]] = []
    f = sys.stdin if use_stdin else open(path, "r", encoding="utf-8")

    with f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "term_frequency" in line:
                continue

            parts = line.split(",", 2)
            if len(parts) < 2:
                continue

            term = parts[0].strip()
            try:
                freq = int(parts[1].strip())
            except Exception:
                continue

            if MIN_FREQUENCY is not None and freq < MIN_FREQUENCY:
                break

            word = normalize_term(term)
            L = len(word)

            if (min_len is None or L >= min_len) and (max_len is None or L <= max_len):
                tasks.append((word, freq, line))

    return tasks


def main() -> None:
    global MAX_VALIDATED, MIN_FREQUENCY, DEBUG
    global VALIDATED_OUTFILE, NONEXISTENT_OUTFILE, HARD_REJECTED_OUTFILE, RETRY_OUTFILE
    global validated_count, processed_count, nonexistent_count, hard_rejected_count, retry_count, last_processed_word

    parser = argparse.ArgumentParser()

    # 2 positional parameters
    parser.add_argument("lang", help="Wiktionary language code (e.g., en, fr, de, ru)")
    parser.add_argument("file", help="Input CSV file (omit .csv allowed) OR '-' for stdin")

    # filters / limits
    parser.add_argument("--min", "-m", dest="min_len", type=int, default=None,
                        help="Minimum word length (inclusive). Default: no minimum")
    parser.add_argument("--max", "-M", dest="max_len", type=int, default=None,
                        help="Maximum word length (inclusive). Default: no maximum")
    parser.add_argument("--size", "-s", dest="size", type=int, default=None,
                        help="Target number of VALIDATED outputs to write. Default: unlimited")
    parser.add_argument("--min-frequency", "-f", dest="min_frequency", type=int, default=None,
                        help="Stop reading input once frequency drops below this value (assumes sorted input)")

    # output (file mode only; stdin mode forces stdout/stderr)
    parser.add_argument("--out", "-o", dest="out", default=None,
                        help="Validated output filename (file mode). If only a name is given, it is written next to the input file.")
    parser.add_argument("--reset", action="store_true",
                        help="Remove generated artifacts before processing (file mode only)")

    # concurrency / debug / cap
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of concurrent Wiktionary requests (default: {DEFAULT_WORKERS})")
    parser.add_argument("--cap", action="store_true",
                        help="Probe both lower/capitalized and emit ONE merged validated line (non-cap) if successful")
    parser.add_argument("--debug", action="store_true",
                        help="Print REST URL + JSON keys to stderr for debugging")

    args = parser.parse_args()

    if args.workers < 1:
        print("ERROR: --workers must be >= 1", file=sys.stderr)
        sys.exit(2)

    DEBUG = args.debug
    MAX_VALIDATED = args.size
    MIN_FREQUENCY = args.min_frequency

    use_stdin = (args.file == "-")

    if not use_stdin:
        try:
            args.file = ensure_csv_input_path(args.file)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(2)

    if use_stdin:
        VALIDATED_OUTFILE = None
        NONEXISTENT_OUTFILE = None
        HARD_REJECTED_OUTFILE = None
        RETRY_OUTFILE = None
    else:
        VALIDATED_OUTFILE, NONEXISTENT_OUTFILE, HARD_REJECTED_OUTFILE, RETRY_OUTFILE = default_out_paths(
            args.file, out_arg_validated=args.out
        )

    if args.reset and not use_stdin:
        reset_artifacts(args.file)
        print("Artifacts removed (--reset).", file=sys.stderr)

    if not use_stdin:
        try:
            open(VALIDATED_OUTFILE, "a", encoding="utf-8").close()
            open(NONEXISTENT_OUTFILE, "a", encoding="utf-8").close()
            open(HARD_REJECTED_OUTFILE, "a", encoding="utf-8").close()
            open(RETRY_OUTFILE, "a", encoding="utf-8").close()
        except Exception as e:
            print(f"ERROR: cannot write outputs: {e}", file=sys.stderr)
            sys.exit(2)

    tasks = parse_input_csv(args.file, args.min_len, args.max_len, use_stdin)

    target_str = str(MAX_VALIDATED) if MAX_VALIDATED is not None else "∞"
    if use_stdin:
        print(
            f"Processing {len(tasks)} candidate words from STDIN. "
            f"--cap: {args.cap}. Workers: {args.workers}. Target validated: {target_str}.",
            file=sys.stderr,
        )
    else:
        print(
            f"Processing {len(tasks)} candidate words. "
            f"--cap: {args.cap}. Workers: {args.workers}.",
            file=sys.stderr,
        )
        print(f"Validated out:   {VALIDATED_OUTFILE}", file=sys.stderr)
        print(f"Nonexistent out: {NONEXISTENT_OUTFILE}", file=sys.stderr)
        print(f"Hard reject out: {HARD_REJECTED_OUTFILE}", file=sys.stderr)
        print(f"Retry out:       {RETRY_OUTFILE}", file=sys.stderr)
        print(f"Target validated: {target_str}", file=sys.stderr)

    # reset run buffers/counters
    validated_count = 0
    processed_count = 0
    nonexistent_count = 0
    hard_rejected_count = 0
    retry_count = 0
    last_processed_word = None
    validated_out_lines.clear()
    nonexistent_lines.clear()
    hard_rejected_lines.clear()
    retry_lines.clear()

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for task in tasks:
            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break
            futures.append(executor.submit(check_wiktionary, task, args.lang, args.cap))

        for i, future in enumerate(as_completed(futures), 1):
            try:
                future.result()
            except Exception as e:
                # Last-resort: if our worker crashes, put line in hard reject bucket
                print(f"\nERROR in worker thread: {type(e).__name__}: {e}", file=sys.stderr)
                with stats_lock:
                    hard_rejected_lines.append(f"__THREAD_EXCEPTION__,{type(e).__name__}:{e}\n")
                    hard_rejected_count += 1
                    processed_count += 1

            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break

            if i % 100 == 0:
                flush_outputs()
                now = time.time()
                elapsed = now - start_time
                with stats_lock:
                    p = processed_count
                    v = validated_count
                    ne = nonexistent_count
                    rj = hard_rejected_count
                    rt = retry_count
                    lastw = last_processed_word
                avg_ms = (elapsed / p * 1000.0) if p else 0.0
                print(
                    f"\rProcessed:{p}  Validated:{v}/{target_str}  "
                    f"Retry:{rt}  Nonexistent:{ne}  HardRejected:{rj}  "
                    f"Elapsed:{elapsed:.1f}s  Avg:{avg_ms:.1f}ms/word  "
                    f"Last:{lastw or ''}",
                    end="",
                    flush=True,
                    file=sys.stderr,
                )

    flush_outputs()

    elapsed = time.time() - start_time
    with stats_lock:
        p = processed_count
        v = validated_count
        ne = nonexistent_count
        rj = hard_rejected_count
        rt = retry_count
        lastw = last_processed_word
    avg_ms = (elapsed / p * 1000.0) if p else 0.0

    print(
        f"\nDone. Processed:{p}  Validated:{v}/{target_str}  "
        f"Retry:{rt}  Nonexistent:{ne}  HardRejected:{rj}  "
        f"Elapsed:{elapsed:.1f}s  Avg:{avg_ms:.1f}ms/word",
        file=sys.stderr,
    )

    if not use_stdin:
        print(f"Validated file:   {VALIDATED_OUTFILE}", file=sys.stderr)
        print(f"Nonexistent file: {NONEXISTENT_OUTFILE}", file=sys.stderr)
        print(f"Hard reject file: {HARD_REJECTED_OUTFILE}", file=sys.stderr)
        print(f"Retry file:       {RETRY_OUTFILE}", file=sys.stderr)

    if lastw:
        print(f"Last word processed: {lastw}", file=sys.stderr)


if __name__ == "__main__":
    main()
