#!/usr/bin/env python3
import sys
import os
import re
import html
import time
import socket
import argparse
import threading
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# --- GLOBAL SAFETY ---
socket.setdefaulttimeout(15)

# --- GLOBALS / STATE ---
stats_lock = threading.Lock()

# output buffers (flushed periodically)
validated_out_lines = []
rejected_lines = []

# counters / limits
validated_count = 0
processed_count = 0
MAX_VALIDATED = None          # --size / -s (validated target)
MIN_FREQUENCY = None          # --min-frequency / -f (stop reading input when below)
DEBUG = False

DEFAULT_WORKERS = 8

RATE_LIMIT_MAX_RETRIES = 6         # total attempts on 429 (in addition to initial request)
RATE_LIMIT_BASE_DELAY = 0.75       # seconds
RATE_LIMIT_MAX_DELAY = 20.0        # cap


# output file paths (resolved in main)
VALIDATED_OUTFILE = None
REJECTED_OUTFILE = None

# --- HTML stripping for Wiktionary definitions ---
_TAG_RE = re.compile(r"<[^>]+>")

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
    root, ext = os.path.splitext(base)

    if ext == "":
        return path + ".csv"
    if ext.lower() != ".csv":
        raise ValueError(f"Input must be a .csv file (or omit extension). Got: {path}")
    return path


def default_out_paths_v2(input_csv_path: str, out_arg_validated: str | None = None):
    """
    Given foo.csv, default outputs:
      validated: foo-VALIDATED.csv
      rejected:  Rfoo.csv

    If --out/-o is provided:
      - filename only -> next to input
      - path included -> use as-is
    """
    input_dir = os.path.dirname(os.path.abspath(input_csv_path))
    base = os.path.basename(input_csv_path)
    root, ext = os.path.splitext(base)

    # validated
    if out_arg_validated:
        if os.path.dirname(out_arg_validated):
            validated_path = out_arg_validated
        else:
            validated_path = os.path.join(input_dir, out_arg_validated)
    else:
        validated_path = os.path.join(input_dir, f"{root}-VALIDATED.csv")

    # rejected always defaults
    rejected_path = os.path.join(input_dir, f"R{root}.csv")

    return validated_path, rejected_path


def reset_artifacts_v2(input_csv_path: str):
    v, r = default_out_paths_v2(input_csv_path, out_arg_validated=None)
    for p in (v, r):
        if os.path.exists(p):
            os.remove(p)


def strip_html(s: str) -> str:
    """Remove HTML tags and unescape HTML entities."""
    if s is None:
        return ""
    s = html.unescape(s)
    return _TAG_RE.sub("", s)

def normalize_term(term: str) -> str:
    """
    Strip trailing POS suffix like 'plus_ADV' -> 'plus' ONLY when suffix looks like POS tag.
    Keeps internal underscores (e.g. multi_word_terms) intact.
    """
    term = term.strip()
    if "_" not in term:
        return term
    base, suffix = term.rsplit("_", 1)
    # Common POS tags are short uppercase alphabetic
    if suffix.isalpha() and suffix.isupper() and 2 <= len(suffix) <= 6:
        return base
    return term

def resolve_outfile_next_to_input(input_file: str, out_arg: str | None, default_suffix: str) -> str:
    """
    If out_arg is None -> <input_dir>/<input_base><default_suffix>
    If out_arg is a filename only -> <input_dir>/<out_arg>
    If out_arg includes a path -> out_arg as-is
    """
    input_dir = os.path.dirname(os.path.abspath(input_file))
    input_base = os.path.basename(input_file)

    if out_arg:
        if os.path.dirname(out_arg):
            return out_arg
        return os.path.join(input_dir, out_arg)

    return os.path.join(input_dir, input_base + default_suffix)

def reset_artifacts(input_file: str):
    """
    Remove artifacts produced by this script:
    - <input>.validated
    - <input>.rejected.csv
    - word_validation.log (if you kept it from earlier iterations)
    """
    base_dir = os.path.dirname(os.path.abspath(input_file))
    base_name = os.path.basename(input_file)

    validated = os.path.join(base_dir, base_name + ".validated")
    rejected = os.path.join(base_dir, base_name + ".rejected.csv")

    for p in (validated, rejected):
        if os.path.exists(p):
            os.remove(p)

    # optional legacy log cleanup
    if os.path.exists("word_validation.log"):
        os.remove("word_validation.log")

def extract_pos_list(lang_section):
    """Collect all PoS values in the language section, deduped, preserve order."""
    pos_list = []
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

def is_plural_noun_entry(lang_section):
    """
    Robust plural detection:
    - checks definition text after stripping HTML for 'plural of'
    """
    plural_markers = (
        "plural of", "plural form of", "plurals of",
        "pluriel de", "plural von", "plural de", "forma plural"
    )

    for entry in lang_section or []:
        for d in entry.get("definitions", []) or []:
            text = strip_html(d.get("definition", "")).lower()
            if any(m in text for m in plural_markers):
                return True
    return False

def extract_additional_info(pos_list, lang_section, is_answer_candidate: bool):
    """
    Best-effort tags (semicolon-separated).
    Nouns: SINGULAR/PLURAL (based on plural-of detection)
    Verbs: a few 'form-of' markers inferred from definition text
    """
    tags = set()

    def_texts = []
    for entry in lang_section or []:
        for d_obj in entry.get("definitions", []) or []:
            t = strip_html(d_obj.get("definition") or "").lower()
            if t:
                def_texts.append(t)

    def contains_any(needles):
        return any(n in t for t in def_texts for n in needles)

    is_noun = any(p == "NOUN" for p in pos_list)
    is_verb = any(p == "VERB" for p in pos_list)

    if is_noun:
        if is_plural_noun_entry(lang_section):
            tags.add("PLURAL")
        else:
            tags.add("SINGULAR")

    if is_verb:
        # crude but helpful across many languages in Wiktionary phrasing
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

def process_wiktionary_data(lang_section):
    """
    Decide if this word is an answer candidate under your current rules:
      - noun
      - not proper noun (by partOfSpeech containing 'proper noun')
      - not plural-of (robust, after stripping HTML)
    Returns: (is_validated, is_answer_candidate)
    """
    if not lang_section:
        return False, False

    # A word is "validated" if we got a language section at all.
    is_validated = True

    # Answer candidate heuristic (Wordle-ish): NOUN, not proper noun, not plural-of
    is_answer_candidate = False
    for entry in lang_section:
        pos_type = (entry.get("partOfSpeech") or "").lower()
        if "proper noun" in pos_type:
            continue
        if "noun" in pos_type:
            if not is_plural_noun_entry(lang_section):
                is_answer_candidate = True
            break

    return is_validated, is_answer_candidate

def fetch_definition(word):
    """
    Calls Wiktionary REST definition endpoint.
    Retries with exponential backoff on HTTP 429 (rate limited).
    Returns (status_code, json_dict_or_none, url)
    """
    safe_word = quote(word, safe="")
    url = "https://en.wiktionary.org/api/rest_v1/page/definition/{}".format(safe_word)
    headers = {"User-Agent": "WordValidatorBot/1.0"}

    attempt = 0
    while True:
        try:
            resp = requests.get(url, headers=headers, timeout=(5, 10))

            if DEBUG:
                print("[DEBUG] REST {} {} (attempt {})".format(resp.status_code, url, attempt), file=sys.stderr)

            # --- exponential backoff on 429 ---
            if resp.status_code == 429:
                if attempt >= RATE_LIMIT_MAX_RETRIES:
                    return 429, None, url

                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except Exception:
                        delay = None
                else:
                    delay = None

                if delay is None:
                    # exponential backoff with jitter
                    delay = min(RATE_LIMIT_MAX_DELAY, RATE_LIMIT_BASE_DELAY * (2 ** attempt))
                    delay = delay * (0.8 + random.random() * 0.4)  # jitter in [0.8, 1.2]

                if INFO:
                    print("[INFO] 429 rate limited; sleeping {:.2f}s then retrying".format(delay), file=sys.stderr)

                time.sleep(delay)
                attempt += 1
                continue

            if resp.status_code != 200:
                return resp.status_code, None, url

            data = resp.json()

            if DEBUG:
                if isinstance(data, dict):
                    print("[DEBUG] JSON keys: {}".format(list(data.keys())), file=sys.stderr)
                else:
                    print("[DEBUG] JSON type: {}".format(type(data)), file=sys.stderr)

            if not isinstance(data, dict):
                return 200, None, url

            return 200, data, url

        except Exception as e:
            if DEBUG:
                print("[DEBUG] Exception fetching '{}': {}".format(word, e), file=sys.stderr)
            return -1, None, url


def check_wiktionary(task_data, lang_code, use_cap):
    """
    task_data: (word_raw, freq_int, raw_line)
    Writes either to validated_out_lines or rejected_lines.
    """
    global validated_count, processed_count

    word_raw, freq, raw_line = task_data

    # Respect validated target early (avoid extra requests if already hit)
    with stats_lock:
        if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
            return False

    attempts = [word_raw.capitalize(), word_raw] if use_cap else [word_raw]

    status = None
    data = None
    for attempt_word in attempts:
        status, data, _ = fetch_definition(attempt_word)
        if status == 200 and data:
            break

    is_validated = False
    is_answer_candidate = False

    if status == 200 and isinstance(data, dict) and (lang_code in data) and isinstance(data[lang_code], list) and data[lang_code]:
        lang_section = data[lang_code]
        is_validated, is_answer_candidate = process_wiktionary_data(lang_section)

        pos_list = extract_pos_list(lang_section)
        additional = extract_additional_info(pos_list, lang_section, is_answer_candidate)

        # word,freq,pos1,pos2,...,posN,additional
        validated_csv = ",".join([word_raw, str(freq)] + pos_list + [additional]) + "\n"

        with stats_lock:
            # enforce output size limit (validated only)
            if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                processed_count += 1
                return False

            validated_out_lines.append(validated_csv)
            validated_count += 1
            processed_count += 1
        return True

    # Rejected: either 404/not found, rate limited, other http, or parsing mismatch
    reason = "unknown"
    if status == 404:
        reason = "nonexistent"
    elif status == 429:
        reason = "rate_limited"
    elif status is None:
        reason = "no_status"
    elif status == -1:
        reason = "exception"
    else:
        reason = f"http_{status}"

    with stats_lock:
        rejected_lines.append(f"{raw_line},{reason}\n")
        processed_count += 1

    return False

def flush_outputs():
    """Flush buffered validated + rejected lines to disk."""
    with stats_lock:
        if VALIDATED_OUTFILE and validated_out_lines:
            with open(VALIDATED_OUTFILE, "a", encoding="utf-8") as v:
                v.writelines(validated_out_lines)
            validated_out_lines.clear()

        if REJECTED_OUTFILE and rejected_lines:
            with open(REJECTED_OUTFILE, "a", encoding="utf-8") as r:
                r.writelines(rejected_lines)
            rejected_lines.clear()

def parse_input_csv(path, min_len, max_len, use_stdin):
    """
    Reads CSV lines formatted as: term,term_frequency
    Returns tasks: (word, freq_int, raw_line)
    Uses global MIN_FREQUENCY cutoff if set.
    """
    tasks = []
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

def main():
    global MAX_VALIDATED, MIN_FREQUENCY, DEBUG, VALIDATED_OUTFILE, REJECTED_OUTFILE

    parser = argparse.ArgumentParser()

    # only 2 positional parameters
    parser.add_argument("lang", help="Wiktionary language code (e.g., en, fr, de, ru)")
    parser.add_argument("file", help="Input CSV file: term,term_frequency")

    # filters / limits
    parser.add_argument("--min", "-m", dest="min_len", type=int, default=None,
                        help="Minimum word length (inclusive). Default: no minimum")
    parser.add_argument("--max", "-M", dest="max_len", type=int, default=None,
                        help="Maximum word length (inclusive). Default: no maximum")
    parser.add_argument("--size", "-s", dest="size", type=int, default=None,
                        help="Target number of VALIDATED outputs to write. Default: unlimited")
    parser.add_argument("--min-frequency", "-f", dest="min_frequency", type=int, default=None,
                        help="Stop reading input once frequency drops below this value (assumes sorted input)")

    # output
    parser.add_argument("--out", "-o", dest="out", default=None,
                        help="Validated output filename. If only a name is given, it is written next to the input file.")
    parser.add_argument("--reset", action="store_true",
                        help="Remove generated artifacts before processing")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of concurrent Wiktionary requests (default: {})".format(DEFAULT_WORKERS)
    )

    # misc
    parser.add_argument("-cap", action="store_true",
                        help="Try capitalized form first")
    parser.add_argument("--debug", action="store_true",
                        help="Print REST URL + JSON keys to stderr for debugging")

    args = parser.parse_args()

    DEBUG = args.debug
    MAX_VALIDATED = args.size
    MIN_FREQUENCY = args.min_frequency

    use_stdin = (args.file == "-")

    # Normalize input filename (file mode only)
    if not use_stdin:
        try:
            args.file = ensure_csv_input_path(args.file)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(2)

    if use_stdin:
        VALIDATED_OUTFILE = None
        REJECTED_OUTFILE = None
    else:
        VALIDATED_OUTFILE, REJECTED_OUTFILE = default_out_paths_v2(args.file, out_arg_validated=args.out)

    if args.reset and not use_stdin:
        reset_artifacts_v2(args.file)
        print("Artifacts removed (--reset).", file=sys.stderr)

    # touch output files (fail fast if unwritable)
    try:
        open(VALIDATED_OUTFILE, "a", encoding="utf-8").close()
        open(REJECTED_OUTFILE, "a", encoding="utf-8").close()
    except Exception as e:
        print(f"ERROR: cannot write outputs: {e}", file=sys.stderr)
        sys.exit(2)

    tasks = parse_input_csv(args.file, args.min_len, args.max_len, use_stdin)

    target = MAX_VALIDATED if MAX_VALIDATED is not None else "∞"
    print(f"Processing {len(tasks)} candidate words. Cap-first: {args.cap}. Out: {VALIDATED_OUTFILE}")
    print(f"Target validated: {target}. Rejected: {REJECTED_OUTFILE}")

    # Process concurrently
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []

        # Submit tasks, but avoid scheduling a huge amount beyond target
        for task in tasks:
            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break
            futures.append(executor.submit(check_wiktionary, task, args.lang, args.cap))

        # Consume results + progress
        for i, future in enumerate(as_completed(futures), 1):
            future.result()

            # flush + report every 100 completed requests
            if i % 100 == 0:
                flush_outputs()
                with stats_lock:
                    p = processed_count
                    v = validated_count
                print(
                    f"\rProcessed:{p}  Validated:{v}/{target}  LastBatchDone:{i}",
                    end="",
                    flush=True
                )

            # If we reached target, we can stop waiting (but threads already running will finish).
            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break

    flush_outputs()

    with stats_lock:
        p = processed_count
        v = validated_count
        r = "see file"
    print(f"\nDone. Processed:{p}  Validated:{v}/{target}  Rejected:{r}")
    print(f"Validated file: {VALIDATED_OUTFILE}")
    print(f"Rejected file:  {REJECTED_OUTFILE}")

if __name__ == "__main__":
    main()
