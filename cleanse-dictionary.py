#!/usr/bin/env python3
"""
Validate words against Wiktionary REST API and emit CSV artifacts.

Input CSV format (assumed):
  term,term_frequency
  plus_ADV,14199037

Validated output CSV lines:
  <word>,<frequency>,<PoS1>,...,<PoSN>,<additional>

Additional:
  - NOUN: SINGULAR or PLURAL
  - VERB: best-effort tags inferred from definition text

Artifacts (file mode), for input foo.csv:
  foo-VALIDATED.csv     (validated words)
  Nfoo.csv              (404: non-existent)
  Rfoo.csv              (other failures: 429, 5xx, exceptions, etc.)
  foo-REJECTED.csv      (SKIPPED: page exists but DOES NOT contain requested locale section)

STDIN mode:
  file argument "-" reads from STDIN
  validated -> STDOUT
  others    -> STDERR (nonexistent, rejected, locale-skipped)
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
rejected_lines: List[str] = []
nonexistent_lines: List[str] = []
locale_skipped_lines: List[str] = []

validated_count = 0
processed_count = 0
skipped_count = 0
rejected_count = 0
nonexistent_count = 0
last_processed_word: Optional[str] = None

MAX_VALIDATED: Optional[int] = None          # --size / -s (validated output target)
MIN_FREQUENCY: Optional[int] = None          # --min-frequency / -f (stop reading input when below)
DEBUG = False

VALIDATED_OUTFILE: Optional[str] = None      # file path or None -> stdout
REJECTED_OUTFILE: Optional[str] = None       # file path or None -> stderr (R<base>.csv)
NONEXISTENT_OUTFILE: Optional[str] = None    # file path or None -> stderr (N<base>.csv)
LOCALE_REJECTED_OUTFILE: Optional[str] = None  # file path or None -> stderr (<base>-REJECTED.csv)

DEFAULT_WORKERS = 8

# Rate-limit backoff controls
RATE_LIMIT_MAX_RETRIES = 6
RATE_LIMIT_BASE_DELAY = 0.75
RATE_LIMIT_MAX_DELAY = 20.0

# --- HTML stripping for Wiktionary definitions ---
_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(s: Optional[str]) -> str:
    """Remove HTML tags and unescape HTML entities."""
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
    """
    File mode only:
      - "foo"     -> "foo.csv"
      - "foo.csv" -> "foo.csv"
    Rejects other extensions.
    """
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
      foo-VALIDATED.csv      (validated)
      Nfoo.csv               (non-existent 404)
      Rfoo.csv               (other rejects)
      foo-REJECTED.csv       (locale-skipped: no requested lang section)
    """
    input_dir = os.path.dirname(os.path.abspath(input_csv_path))
    base = os.path.basename(input_csv_path)
    root, _ext = os.path.splitext(base)

    # validated
    if out_arg_validated:
        if os.path.dirname(out_arg_validated):
            validated_path = out_arg_validated
        else:
            validated_path = os.path.join(input_dir, out_arg_validated)
    else:
        validated_path = os.path.join(input_dir, f"{root}-VALIDATED.csv")

    nonexistent_path = os.path.join(input_dir, f"N{root}.csv")
    rejected_path = os.path.join(input_dir, f"R{root}.csv")
    locale_rejected_path = os.path.join(input_dir, f"{root}-REJECTED.csv")

    return validated_path, nonexistent_path, rejected_path, locale_rejected_path


def reset_artifacts(input_csv_path: str) -> None:
    v, n, r, lr = default_out_paths(input_csv_path, out_arg_validated=None)
    for p in (v, n, r, lr):
        if os.path.exists(p):
            os.remove(p)


def extract_pos_list(lang_section: List[Dict[str, Any]]) -> List[str]:
    """Collect all PoS values in the language section, deduped, preserve order."""
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
    """
    Detect plural noun "form-of" entries by scanning stripped definition text.
    Example: "<a>plural</a> of <a>year</a>" -> "plural of year".
    """
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


def extract_additional_info(pos_list: List[str], lang_section: List[Dict[str, Any]]) -> str:
    """
    Additional info tags (semicolon-separated).
      - NOUN: SINGULAR/PLURAL
      - VERB: best-effort markers inferred from definition text
    """
    tags = set()

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
    Calls Wiktionary REST definition endpoint.
    Retries with exponential backoff on HTTP 429 (rate limited).
    Returns (status_code, json_dict_or_none, url)
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

            # --- exponential backoff on 429 ---
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
                    delay *= (0.8 + random.random() * 0.4)  # jitter in [0.8, 1.2]

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
                else:
                    print(f"[DEBUG] JSON type: {type(data)}", file=sys.stderr)

            if not isinstance(data, dict):
                return 200, None, url

            return 200, data, url

        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Exception fetching '{word}': {type(e).__name__}: {e}", file=sys.stderr)
            return -1, None, url


def check_wiktionary(task_data: Tuple[str, int, str], lang_code: str, use_cap: bool) -> bool:
    """
    task_data: (word_raw, freq_int, raw_line)

    With --cap:
      1) Try non-capitalized first.
      2) If validated, also probe capitalized:
         - If capitalized validates: record it too.
         - If capitalized 404: do nothing.
         - If capitalized error: skip (do not write R/N).
      3) If non-capitalized 404: try capitalized normally (R/N/locale-reject as usual).
    """
    global validated_count, processed_count, skipped_count, rejected_count, nonexistent_count, last_processed_word

    word_raw, freq, raw_line = task_data

    def record_validated(out_word: str, section: List[Dict[str, Any]]) -> bool:
        nonlocal freq
        pos_list = extract_pos_list(section)
        additional = extract_additional_info(pos_list, section)
        validated_csv = ",".join([out_word, str(freq)] + pos_list + [additional]) + "\n"

        with stats_lock:
            if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                return False
            validated_out_lines.append(validated_csv)
            return True

    def handle_200(data: Dict[str, Any], out_word: str) -> Tuple[bool, bool]:
        """
        Returns (validated_success, locale_missing)
        """
        section = data.get(lang_code)
        if not (isinstance(section, list) and section):
            return (False, True)
        ok = record_validated(out_word, section)
        return (ok, False)

    def record_locale_reject() -> None:
        # keep your "rejected schema": add reason column
        with stats_lock:
            locale_skipped_lines.append(f"{raw_line},wrong_locale\n")

    def record_nonexistent() -> None:
        with stats_lock:
            nonexistent_lines.append(f"{raw_line}\n")

    def record_rejected(reason: str) -> None:
        with stats_lock:
            rejected_lines.append(f"{raw_line},{reason}\n")

    # If already hit target validated, skip work
    with stats_lock:
        if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
            return False

    lower_word = word_raw
    cap_word = word_raw.capitalize()

    # ---- Helper: run a single probe ----
    def probe(w: str) -> Tuple[int, Optional[Dict[str, Any]], str]:
        return fetch_definition(w)

    # ---- 1) Always probe LOWER first if --cap is set, else probe just word_raw ----
    first_word = lower_word if use_cap else word_raw
    status1, data1, url1 = probe(first_word)

    # ---- Case A: first probe 200 ----
    if status1 == 200 and isinstance(data1, dict) and data1:
        validated1, locale_missing1 = handle_200(data1, first_word)

        with stats_lock:
            processed_count += 1
            last_processed_word = word_raw
            if validated1:
                validated_count += 1
            elif locale_missing1:
                skipped_count += 1
            else:
                # 200 but unusable JSON treated as rejected
                rejected_count += 1

        if locale_missing1:
            record_locale_reject()
            return False

        if not validated1:
            # 200 but couldn't record (e.g., hit MAX_VALIDATED)
            return False

        # If validated and --cap specified: probe Capitalized too
        if use_cap and cap_word != first_word:
            status2, data2, url2 = probe(cap_word)

            if DEBUG:
                print(f"[DEBUG] CAP probe result {status2} for {cap_word}", file=sys.stderr)

            # If cap validates, record it as well
            if status2 == 200 and isinstance(data2, dict) and data2:
                validated2, locale_missing2 = handle_200(data2, cap_word)

                # Rules for second probe:
                # - locale missing: treat as locale reject? (I'd keep it consistent and record to <base>-REJECTED)
                #   If you'd rather skip silently, tell me.
                if locale_missing2:
                    with stats_lock:
                        skipped_count += 1
                    record_locale_reject()
                elif validated2:
                    with stats_lock:
                        validated_count += 1
                # else: 200 but could not record (MAX_VALIDATED) -> ignore

            elif status2 == 404:
                # "do nothing for nonexistent" when already succeeded on lower
                pass
            else:
                # "if error skip" when already succeeded on lower
                pass

        return True

    # ---- Case B: first probe 404 ----
    if status1 == 404:
        if use_cap and cap_word != first_word:
            # Try capitalized normally
            status2, data2, url2 = probe(cap_word)

            if status2 == 200 and isinstance(data2, dict) and data2:
                validated2, locale_missing2 = handle_200(data2, cap_word)

                with stats_lock:
                    processed_count += 1
                    last_processed_word = word_raw
                    if validated2:
                        validated_count += 1
                    elif locale_missing2:
                        skipped_count += 1
                    else:
                        rejected_count += 1

                if locale_missing2:
                    record_locale_reject()
                    return False
                return bool(validated2)

            if status2 == 404:
                with stats_lock:
                    processed_count += 1
                    last_processed_word = word_raw
                    nonexistent_count += 1
                record_nonexistent()
                return False

            # other errors on cap in this branch are handled normally -> rejected
            reason = "rate_limited" if status2 == 429 else ("exception" if status2 == -1 else f"http_{status2}")
            with stats_lock:
                processed_count += 1
                last_processed_word = word_raw
                rejected_count += 1
            record_rejected(reason)
            return False

        # no cap mode: 404 is nonexistent
        with stats_lock:
            processed_count += 1
            last_processed_word = word_raw
            nonexistent_count += 1
        record_nonexistent()
        return False

    # ---- Case C: first probe error (429/5xx/exception/etc.) handled normally ----
    reason = "rate_limited" if status1 == 429 else ("exception" if status1 == -1 else f"http_{status1}")
    with stats_lock:
        processed_count += 1
        last_processed_word = word_raw
        rejected_count += 1
    record_rejected(reason)
    return False

def flush_outputs() -> None:
    """Flush buffered lines to disk or streams."""
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

        if rejected_lines:
            if REJECTED_OUTFILE:
                with open(REJECTED_OUTFILE, "a", encoding="utf-8") as r:
                    r.writelines(rejected_lines)
            else:
                sys.stderr.writelines(rejected_lines)
                sys.stderr.flush()
            rejected_lines[:] = []

        if locale_skipped_lines:
            if LOCALE_REJECTED_OUTFILE:
                with open(LOCALE_REJECTED_OUTFILE, "a", encoding="utf-8") as lr:
                    lr.writelines(locale_skipped_lines)
            else:
                sys.stderr.writelines(locale_skipped_lines)
                sys.stderr.flush()
            locale_skipped_lines[:] = []


def parse_input_csv(path: str, min_len: Optional[int], max_len: Optional[int], use_stdin: bool) -> List[Tuple[str, int, str]]:
    """
    Reads CSV lines formatted as:
      term,term_frequency
    Applies:
      - normalize_term()
      - length filter (if provided)
      - min-frequency cutoff (stops reading once below threshold; assumes descending sorted input)
    Returns tasks: (word, freq, raw_line)
    """
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
    global VALIDATED_OUTFILE, NONEXISTENT_OUTFILE, REJECTED_OUTFILE, LOCALE_REJECTED_OUTFILE
    global validated_count, processed_count, skipped_count, rejected_count, nonexistent_count, last_processed_word

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

    # concurrency / debug
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of concurrent Wiktionary requests (default: {DEFAULT_WORKERS})")
    parser.add_argument("--cap", action="store_true",
                    help="If set: try lowercase first; if validated, also probe Capitalized (do not penalize 404/errors on the second probe)")
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

    # Normalize input filename in file mode
    if not use_stdin:
        try:
            args.file = ensure_csv_input_path(args.file)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(2)

    # Outputs
    if use_stdin:
        VALIDATED_OUTFILE = None
        NONEXISTENT_OUTFILE = None
        REJECTED_OUTFILE = None
        LOCALE_REJECTED_OUTFILE = None
    else:
        VALIDATED_OUTFILE, NONEXISTENT_OUTFILE, REJECTED_OUTFILE, LOCALE_REJECTED_OUTFILE = default_out_paths(
            args.file, out_arg_validated=args.out
        )

    # Reset (file mode only)
    if args.reset and not use_stdin:
        reset_artifacts(args.file)
        print("Artifacts removed (--reset).", file=sys.stderr)

    # Touch output files (file mode)
    if not use_stdin:
        try:
            open(VALIDATED_OUTFILE, "a", encoding="utf-8").close()
            open(NONEXISTENT_OUTFILE, "a", encoding="utf-8").close()
            open(REJECTED_OUTFILE, "a", encoding="utf-8").close()
            open(LOCALE_REJECTED_OUTFILE, "a", encoding="utf-8").close()
        except Exception as e:
            print(f"ERROR: cannot write outputs: {e}", file=sys.stderr)
            sys.exit(2)

    # Parse input
    tasks = parse_input_csv(args.file, args.min_len, args.max_len, use_stdin)

    target_str = str(MAX_VALIDATED) if MAX_VALIDATED is not None else "∞"
    if use_stdin:
        print(
            f"Processing {len(tasks)} candidate words from STDIN. "
            f"Cap-first: {args.cap}. Workers: {args.workers}. Target validated: {target_str}.",
            file=sys.stderr
        )
    else:
        print(
            f"Processing {len(tasks)} candidate words. "
            f"Cap-first: {args.cap}. Workers: {args.workers}.",
            file=sys.stderr
        )
        print(f"Validated out:      {VALIDATED_OUTFILE}", file=sys.stderr)
        print(f"Nonexistent out:    {NONEXISTENT_OUTFILE}", file=sys.stderr)
        print(f"Rejected out:       {REJECTED_OUTFILE}", file=sys.stderr)
        print(f"Locale-rejected out:{LOCALE_REJECTED_OUTFILE}", file=sys.stderr)
        print(f"Target validated: {target_str}", file=sys.stderr)

    # Reset run counters/buffers (in case of reuse in same process)
    validated_count = 0
    processed_count = 0
    skipped_count = 0
    rejected_count = 0
    nonexistent_count = 0
    last_processed_word = None
    validated_out_lines.clear()
    nonexistent_lines.clear()
    rejected_lines.clear()
    locale_skipped_lines.clear()

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for task in tasks:
            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break
            futures.append(executor.submit(check_wiktionary, task, args.lang, args.cap))

        for i, future in enumerate(as_completed(futures), 1):
            # CRITICAL: surfaces worker exceptions
            try:
                future.result()
            except Exception as e:
                print(f"\nERROR in worker thread: {type(e).__name__}: {e}", file=sys.stderr)
                with stats_lock:
                    rejected_lines.append(f"__THREAD_EXCEPTION__,{type(e).__name__}:{e}\n")
                    rejected_count += 1
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
                    s = skipped_count
                    ne = nonexistent_count
                    rj = rejected_count
                    lastw = last_processed_word

                avg_ms = (elapsed / p * 1000.0) if p else 0.0
                print(
                    f"\rProcessed:{p}  Validated:{v}/{target_str}  "
                    f"LocaleSkipped:{s}  Nonexistent:{ne}  Retry:{rj}  "
                    f"Elapsed:{elapsed:.1f}s  Avg:{avg_ms:.1f}ms/word  "
                    f"Last:{lastw or ''}",
                    end="",
                    flush=True,
                    file=sys.stderr
                )

    flush_outputs()

    elapsed = time.time() - start_time
    with stats_lock:
        p = processed_count
        v = validated_count
        s = skipped_count
        ne = nonexistent_count
        rj = rejected_count
        lastw = last_processed_word

    avg_ms = (elapsed / p * 1000.0) if p else 0.0

    print(
        f"\nDone. Processed:{p}  Validated:{v}/{target_str}  "
        f"LocaleSkipped:{s}  Nonexistent:{ne}  Retry:{rj}  "
        f"Elapsed:{elapsed:.1f}s  Avg:{avg_ms:.1f}ms/word",
        file=sys.stderr
    )

    if not use_stdin:
        print(f"Validated file:       {VALIDATED_OUTFILE}", file=sys.stderr)
        print(f"Nonexistent file:     {NONEXISTENT_OUTFILE}", file=sys.stderr)
        print(f"Rejected file:        {REJECTED_OUTFILE}", file=sys.stderr)
        print(f"Locale-rejected file: {LOCALE_REJECTED_OUTFILE}", file=sys.stderr)

if __name__ == "__main__":
    main()
