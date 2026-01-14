#!/usr/bin/env python3
import sys
import os
import re
import html
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

# buffered outputs (flushed periodically)
validated_out_lines = []
rejected_lines = []

# counters / limits
validated_count = 0
processed_count = 0
last_processed_word = None

MAX_VALIDATED = None          # --size / -s (validated target)
MIN_FREQUENCY = None          # --min-frequency / -f (stop reading input when below)
DEBUG = False

# output destinations (None means stream)
VALIDATED_OUTFILE = None      # file path or None -> stdout
REJECTED_OUTFILE = None       # file path or None -> stderr

# --- HTML stripping for Wiktionary definitions ---
_TAG_RE = re.compile(r"<[^>]+>")

def strip_html(s):
    """Remove HTML tags and unescape HTML entities."""
    if s is None:
        return ""
    s = html.unescape(s)
    return _TAG_RE.sub("", s)

def normalize_term(term):
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

def resolve_outfile_next_to_input(input_file, out_arg, default_suffix):
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

def reset_artifacts(input_file):
    """
    Remove artifacts produced by this script (file mode only):
    - <input>.validated (default)
    - <input>.rejected.csv
    """
    base_dir = os.path.dirname(os.path.abspath(input_file))
    base_name = os.path.basename(input_file)

    default_validated = os.path.join(base_dir, base_name + ".validated")
    default_rejected = os.path.join(base_dir, base_name + ".rejected.csv")

    for p in (default_validated, default_rejected):
        if os.path.exists(p):
            os.remove(p)

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
    Robust plural detection using definition text AFTER stripping HTML.
    Example: "<a>plural</a> of <a>year</a>" becomes "plural of year".
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

def extract_additional_info(pos_list, lang_section, is_answer_candidate):
    """
    Tags (semicolon-separated).
    Nouns: SINGULAR/PLURAL
    Verbs: some best-effort markers inferred from definition text
    Always adds ANSWER_CANDIDATE if heuristic passed.
    """
    tags = set()

    def_texts = []
    for entry in lang_section or []:
        for d_obj in entry.get("definitions", []) or []:
            t = strip_html(d_obj.get("definition") or "").lower()
            if t:
                def_texts.append(t)

    def contains_any(needles):
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

    if is_answer_candidate:
        tags.add("ANSWER_CANDIDATE")

    return ";".join(sorted(tags))

def process_wiktionary_data(lang_section):
    """
    Decide if this word is an answer candidate under your current rules:
      - noun
      - not proper noun (partOfSpeech contains 'proper noun')
      - not plural-of (HTML-stripped check)
    Returns: (is_validated, is_answer_candidate)
    """
    if not lang_section:
        return False, False

    is_validated = True

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
    Returns (status_code, json_dict_or_none, url)
    """
    safe_word = quote(word, safe="")
    url = "https://en.wiktionary.org/api/rest_v1/page/definition/{}".format(safe_word)
    headers = {"User-Agent": "WordValidatorBot/1.0"}

    try:
        resp = requests.get(url, headers=headers, timeout=(5, 10))
        if DEBUG:
            print("[DEBUG] REST {} {}".format(resp.status_code, url), file=sys.stderr)

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
    global validated_count, processed_count, last_processed_word

    word_raw, freq, raw_line = task_data

    # Respect validated target early (avoid extra requests if already hit)
    with stats_lock:
        if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
            return False

    attempts = [word_raw.capitalize(), word_raw] if use_cap else [word_raw]

    status = None
    data = None
    for attempt_word in attempts:
        status, data, _url = fetch_definition(attempt_word)
        if status == 200 and data:
            break

    # validated means: we got JSON and it has a non-empty section for the requested language
    if status == 200 and isinstance(data, dict) and (lang_code in data) and isinstance(data[lang_code], list) and data[lang_code]:
        lang_section = data[lang_code]
        _is_validated, is_answer_candidate = process_wiktionary_data(lang_section)

        pos_list = extract_pos_list(lang_section)
        additional = extract_additional_info(pos_list, lang_section, is_answer_candidate)

        # word,freq,pos1,pos2,...,posN,additional
        validated_csv = ",".join([word_raw, str(freq)] + pos_list + [additional]) + "\n"

        with stats_lock:
            # enforce output size limit (validated only)
            if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                last_processed_word = word_raw
                processed_count += 1
                return False

            validated_out_lines.append(validated_csv)
            validated_count += 1
            processed_count += 1
            last_processed_word = word_raw

        return True

    # Rejected
    if status == 404:
        reason = "nonexistent"
    elif status == 429:
        reason = "rate_limited"
    elif status == -1:
        reason = "exception"
    elif status is None:
        reason = "no_status"
    else:
        reason = "http_{}".format(status)

    with stats_lock:
        rejected_lines.append("{},{}\n".format(raw_line, reason))
        processed_count += 1
        last_processed_word = word_raw

    return False

def flush_outputs():
    """Flush buffered validated + rejected lines to disk or streams."""
    with stats_lock:
        if validated_out_lines:
            if VALIDATED_OUTFILE:
                with open(VALIDATED_OUTFILE, "a", encoding="utf-8") as v:
                    v.writelines(validated_out_lines)
            else:
                sys.stdout.writelines(validated_out_lines)
                sys.stdout.flush()
            validated_out_lines[:] = []

        if rejected_lines:
            if REJECTED_OUTFILE:
                with open(REJECTED_OUTFILE, "a", encoding="utf-8") as r:
                    r.writelines(rejected_lines)
            else:
                sys.stderr.writelines(rejected_lines)
                sys.stderr.flush()
            rejected_lines[:] = []

def parse_input_csv(path, min_len, max_len, use_stdin):
    """
    Reads CSV lines formatted as:
      term,term_frequency
      plus_ADV,14199037
    Applies:
      - normalize_term()
      - length filter (if provided)
      - min-frequency cutoff (stops reading once below threshold; assumes descending sorted)
    Returns list of tasks: (word, freq_int, raw_line_without_newline)
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

            # stop reading further once we drop below min frequency (sorted input)
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
    parser.add_argument("file", help="Input CSV file 'term,term_frequency' OR '-' for stdin")

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

    # Outputs
    if use_stdin:
        VALIDATED_OUTFILE = None   # stdout
        REJECTED_OUTFILE = None    # stderr
    else:
        VALIDATED_OUTFILE = resolve_outfile_next_to_input(args.file, args.out, ".validated")
        REJECTED_OUTFILE = resolve_outfile_next_to_input(args.file, None, ".rejected.csv")

    # Reset (file mode only)
    if args.reset and not use_stdin:
        reset_artifacts(args.file)
        print("Artifacts removed (--reset).", file=sys.stderr)

    # Touch output files (file mode)
    if not use_stdin:
        try:
            open(VALIDATED_OUTFILE, "a", encoding="utf-8").close()
            open(REJECTED_OUTFILE, "a", encoding="utf-8").close()
        except Exception as e:
            print("ERROR: cannot write outputs: {}".format(e), file=sys.stderr)
            sys.exit(2)

    tasks = parse_input_csv(args.file, args.min_len, args.max_len, use_stdin)

    target = MAX_VALIDATED if MAX_VALIDATED is not None else "∞"
    if not use_stdin:
        print("Processing {} candidate words. Cap-first: {}. Out: {}".format(len(tasks), args.cap, VALIDATED_OUTFILE), file=sys.stderr)
        print("Target validated: {}. Rejected: {}".format(target, REJECTED_OUTFILE), file=sys.stderr)
    else:
        print("Processing {} candidate words from STDIN. Cap-first: {}. Output: STDOUT (validated), STDERR (rejected)".format(len(tasks), args.cap), file=sys.stderr)
        print("Target validated: {}".format(target), file=sys.stderr)

    # Process concurrently
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        future_to_word = {}

        # Submit tasks, but avoid scheduling too far beyond target
        for task in tasks:
            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break
            fut = executor.submit(check_wiktionary, task, args.lang, args.cap)
            futures.append(fut)
            future_to_word[fut] = task[0]

        # Consume results + progress
        for i, fut in enumerate(as_completed(futures), 1):
            # Stop waiting once we reached the validated target
            with stats_lock:
                if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                    break

            if i % 100 == 0:
                flush_outputs()
                with stats_lock:
                    p = processed_count
                    v = validated_count
                    lastw = last_processed_word
                current_hint = future_to_word.get(fut) or ""
                # progress should not pollute stdout in stdin mode; always print to stderr
                print(
                    "\rProcessed:{}  Validated:{}/{}  LastCompleted:{}  CurrentFuture:{}"
                    .format(p, v, target, (lastw or ""), current_hint),
                    end="",
                    flush=True,
                    file=sys.stderr
                )

    flush_outputs()

    with stats_lock:
        p = processed_count
        v = validated_count
        lastw = last_processed_word

    # Final summary goes to stderr so it won't mix with stdout validated stream
    if use_stdin:
        print("\nDone. Processed:{}  Validated:{}/{}  (Rejected -> STDERR)".format(p, v, target), file=sys.stderr)
    else:
        print("\nDone. Processed:{}  Validated:{}/{}".format(p, v, target), file=sys.stderr)
        print("Validated file: {}".format(VALIDATED_OUTFILE), file=sys.stderr)
        print("Rejected file:  {}".format(REJECTED_OUTFILE), file=sys.stderr)

    if lastw:
        print("Last word processed: {}".format(lastw), file=sys.stderr)

if __name__ == "__main__":
    main()
