import sys
import time
import argparse
import os
import threading
import requests
import logging
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global safety net for hanging sockets
socket.setdefaulttimeout(15)

logging.basicConfig(filename="word_validation.log", level=logging.INFO)

# --- GLOBALS ---
stats_lock = threading.Lock()
files_data = {}
rej_dictionary_lines = []
validated_out_lines = []
VALIDATED_OUTFILE = None

validated_count = 0
processed_count = 0


def reset_artifacts(input_file):
    """
    Remove all generated output artifacts:
    - answers-*.txt
    - dictionary-*.txt
    - rejects.txt
    - log file
    - *.validated output next to input
    """
    base_dir = os.path.dirname(os.path.abspath(input_file))
    base_name = os.path.basename(input_file)

    patterns = [
        "answers-",
        "dictionary-",
    ]

    for fn in os.listdir(base_dir or "."):
        full = os.path.join(base_dir, fn)

        # per-length outputs
        if any(fn.startswith(p) and fn.endswith(".txt") for p in patterns):
            os.remove(full)

        # rejects
        elif fn == "rejects.txt":
            os.remove(full)

        # validated outputs tied to this input
        elif fn == base_name + ".validated":
            os.remove(full)

    # log file (global)
    if os.path.exists("word_validation.log"):
        os.remove("word_validation.log")


def get_all_file_counts(min_len=None, max_len=None):
    """Calculates current line counts for all output files."""
    stats = []

    # If min/max aren't provided, infer from lengths seen so far.
    if min_len is None or max_len is None:
        if files_data:
            seen = sorted(files_data.keys())
            if min_len is None:
                min_len = seen[0]
            if max_len is None:
                max_len = seen[-1]
        else:
            return ""

    for length in range(min_len, max_len + 1):
        ans_fn = f"answers-{length}.txt"
        dict_fn = f"dictionary-{length}.txt"

        a_count = sum(1 for _ in open(ans_fn)) if os.path.exists(ans_fn) else 0
        d_count = sum(1 for _ in open(dict_fn)) if os.path.exists(dict_fn) else 0

        if a_count > 0 or d_count > 0:
            stats.append(f"{length}L: (A:{a_count}/D:{d_count})")
    return " | ".join(stats)


def process_wiktionary_data(lang_data):
    """Return (fallback_pos, raw_pos_name, is_answer_candidate)."""
    plural_markers = ["plural of", "formule plurielle", "plural von", "plural de", "forma plural"]
    proper_markers = ["proper noun", "nom propre", "eigenname", "nombre propio", "surname", "given name"]

    if not lang_data:
        return None, None, False

    for entry in lang_data:
        pos_type = entry.get("partOfSpeech", "").lower()
        is_noun_pos = any(x in pos_type for x in ["noun", "nom", "sustantivo", "substantiv"])
        is_proper = any(m in pos_type for m in proper_markers)

        # Your existing answer logic: noun + not proper + not plural-of
        if is_noun_pos and not is_proper:
            is_plural = False
            for d_obj in entry.get("definitions", []):
                text = d_obj.get("definition", "").lower()
                if any(marker in text for marker in plural_markers):
                    is_plural = True
                    break
            if not is_plural:
                return "NOUN", pos_type, True

    first_entry = lang_data[0]
    fallback_pos = first_entry.get("partOfSpeech", "UNKNOWN").upper()
    return fallback_pos, fallback_pos.lower(), False


def extract_pos_list(lang_section):
    """Collect all PoS values in the language section, deduped, preserved order."""
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


def extract_additional_info(pos_list, lang_section):
    """Best-effort morphological tags from definition text."""
    tags = set()

    # Scan definitions once
    def_texts = []
    for entry in lang_section or []:
        for d_obj in entry.get("definitions", []):
            t = (d_obj.get("definition") or "").lower()
            if t:
                def_texts.append(t)

    def contains_any(needles):
        return any(n in t for t in def_texts for n in needles)

    is_noun = any("NOUN" in p or p == "NOM" for p in pos_list)
    is_verb = any("VERB" in p for p in pos_list)

    if is_noun:
        # plural detection
        if contains_any(["plural of", "pluriel de", "plural von", "plural de", "forma plural"]):
            tags.add("PLURAL")
        else:
            tags.add("SINGULAR")

    if is_verb:
        # common 'form-of' signals; imperfect but useful
        if contains_any(["past participle", "participle past", "participe passé", "partizip ii"]):
            tags.add("PAST_PART")
            tags.add("INFLECTED_FORM")
        if contains_any(["present participle", "gerund", "participe présent", "partizip i"]):
            tags.add("PRES_PART")
            tags.add("INFLECTED_FORM")
        if contains_any(["past tense", "simple past", "preterite", "prétérit", "imparfait"]):
            tags.add("PAST")
            tags.add("INFLECTED_FORM")
        if contains_any(["third-person singular", "3rd-person singular", "3rd person singular"]):
            tags.add("3PS")
            tags.add("INFLECTED_FORM")
        if contains_any(["imperative"]):
            tags.add("IMP")
            tags.add("INFLECTED_FORM")
        if contains_any(["infinitive"]):
            tags.add("INF")
        if contains_any(["subjunctive"]):
            tags.add("SUBJ")
            tags.add("INFLECTED_FORM")
        if contains_any(["form of", "inflection of", "conjugation of", "conjugated form of"]):
            tags.add("FORM_OF")
            tags.add("INFLECTED_FORM")

    return ";".join(sorted(tags))


def fetch_definition(word, lang_arg):
    from urllib.parse import quote

    safe_word = quote(word, safe="")
    url = f"https://en.wiktionary.org/api/rest_v1/page/definition/{safe_word}"
    headers = {"User-Agent": "WordValidatorBot/1.0"}

    try:
        response = requests.get(url, headers=headers, timeout=(5, 10))

        if DEBUG:
            print(
                f"[DEBUG] REST {response.status_code} {url}",
                file=sys.stderr,
            )

        if response.status_code != 200:
            return None

        data = response.json()

        if DEBUG:
            if isinstance(data, dict):
                print(
                    f"[DEBUG] JSON keys: {list(data.keys())}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"[DEBUG] JSON type: {type(data)}",
                    file=sys.stderr,
                )

        if not isinstance(data, dict):
            return None

        # language code must exist and be non-empty
        if lang_arg in data and isinstance(data[lang_arg], list) and data[lang_arg]:
            return data[lang_arg]

        return None

    except Exception as e:
        if DEBUG:
            print(
                f"[DEBUG] Exception fetching {word}: {e}",
                file=sys.stderr,
            )
        return None


def check_wiktionary(task_data, lang_code, use_cap):
    global processed_count

    word_raw, freq, raw_line = task_data
    length = len(word_raw)
    attempts = [word_raw.capitalize(), word_raw] if use_cap else [word_raw]

    lang_section = None
    for attempt_word in attempts:
        lang_section = fetch_definition(attempt_word, lang_code)
        if lang_section:
            break

    if lang_section:
        final_pos, raw_pos_name, is_answer = process_wiktionary_data(lang_section)

        # Existing per-length outputs
        formatted_line = f"{word_raw} {freq} {final_pos}\n"

        # New unified validated output line:
        pos_list = extract_pos_list(lang_section)
        additional = extract_additional_info(pos_list, lang_section)
        if is_answer:
            additional = (additional + (";" if additional else "") + "ANSWER_CANDIDATE")

        # word,freq,pos1,pos2,...,posN,additional
        validated_csv = ",".join([word_raw, str(freq)] + pos_list + [additional]) + "\n"

        with stats_lock:
            global validated_count

            # Enforce output size limit
            if MAX_VALIDATED is not None and validated_count >= MAX_VALIDATED:
                return False

            if length not in files_data:
                files_data[length] = {"answers": [], "dictionary": []}

            files_data[length]["dictionary"].append(formatted_line)
            if is_answer:
                files_data[length]["answers"].append(formatted_line)

            validated_out_lines.append(validated_csv)
            validated_count += 1
            processed_count += 1

        return True
    else:
        with stats_lock:

            rej_dictionary_lines.append(f"{raw_line},nonexistent\n")
            processed_count += 1
        return False


def save_files():
    with stats_lock:
        # Write existing outputs
        for length, data in files_data.items():
            for cat in ["answers", "dictionary"]:
                fn = f"answers-{length}.txt" if cat == "answers" else f"dictionary-{length}.txt"
                if data[cat]:
                    with open(fn, "a", encoding="utf-8") as out:
                        out.writelines(data[cat])
                    data[cat].clear()

        # Write rejects
        if rej_dictionary_lines:
            with open("rejects.txt", "a", encoding="utf-8") as r:
                r.writelines(rej_dictionary_lines)
            rej_dictionary_lines.clear()

        # Write unified validated output
        if VALIDATED_OUTFILE and validated_out_lines:
            with open(VALIDATED_OUTFILE, "a", encoding="utf-8") as v:
                v.writelines(validated_out_lines)
            validated_out_lines.clear()


def main():
    global VALIDATED_OUTFILE

    parser = argparse.ArgumentParser()

    # positional only: language + input file
    parser.add_argument("lang")
    parser.add_argument("file")

    # flags (with short forms)
    parser.add_argument("--min", "-m", dest="min_len", type=int, default=None,
                        help="Minimum word length (inclusive)")
    parser.add_argument("--max", "-M", dest="max_len", type=int, default=None,
                        help="Maximum word length (inclusive)")
    parser.add_argument("--size", "-s", dest="size", type=int, default=None,
                        help="Maximum number of validated words to output")

    parser.add_argument("--min-frequency", "-f", dest="min_frequency", type=int, default=None,
                        help="Stop processing once word frequency drops below this value")

    parser.add_argument("--out", "-o", dest="out", default=None,
                        help="Unified validated output file (CSV-like)")

    parser.add_argument("-cap", action="store_true",
                        help="Try capitalized form first")

    parser.add_argument("--reset", action="store_true",
                    help="Remove all generated artifacts before processing")

    parser.add_argument("--debug", action="store_true",
                    help="Debug Wiktionary REST calls and responses")

    args = parser.parse_args()

    input_dir = os.path.dirname(os.path.abspath(args.file))
    input_base = os.path.basename(args.file)

    if args.out:
        # if user supplied just a filename, put it next to input
        if os.path.dirname(args.out):
            VALIDATED_OUTFILE = args.out
        else:
            VALIDATED_OUTFILE = os.path.join(input_dir, args.out)
    else:
        VALIDATED_OUTFILE = os.path.join(input_dir, input_base + ".validated")

    global MAX_VALIDATED, MIN_FREQUENCY
    MAX_VALIDATED = args.size
    MIN_FREQUENCY = args.min_frequency

    if args.reset:
        reset_artifacts(args.file)
        print("Artifacts removed (--reset).")

    global DEBUG
    DEBUG = args.debug

    # touch output (fail fast if unwritable)
    try:
        open(VALIDATED_OUTFILE, "a", encoding="utf-8").close()
    except Exception as e:
        print(f"ERROR: cannot write to output file {VALIDATED_OUTFILE}: {e}", file=sys.stderr)
        sys.exit(2)

    tasks = []
    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "term_frequency" in line:
                continue
            try:
                parts = line.split(",")
                word_raw = parts[0].split("_")[0]
                freq = int(parts[1])

                # frequency cutoff (assumes descending-sorted input)
                if MIN_FREQUENCY is not None and freq < MIN_FREQUENCY:
                    break

                L = len(word_raw)
                if (args.min_len is None or L >= args.min_len) and (args.max_len is None or L <= args.max_len):
                    tasks.append((word_raw, freq, line))
            except Exception:
                continue

    if args.size is not None and args.size >= 0:
        tasks = tasks[: args.size]

    print(f"Processing {len(tasks)} words. Cap-first: {args.cap}. Out: {VALIDATED_OUTFILE}")

    count = 0
    file_stats = ""
    
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(check_wiktionary, task, args.lang, args.cap) for task in tasks]
        future_to_word = {fut: task for fut, task in zip(futures, tasks)}

        for future in as_completed(future_to_word):
            task_data = future_to_word[future]
            count += 1

            if count % 100 == 0:
                save_files()
                file_stats = get_all_file_counts(args.min_len, args.max_len)

            target = MAX_VALIDATED if MAX_VALIDATED is not None else "∞"

            with stats_lock:
                p = processed_count
                v = validated_count

            print(
                f"\rProcessed:{p}  Validated:{v}/{target}  "
                f"Current:{task_data[0]:<15}  | {file_stats}",
                end="",
                flush=True
            )
            # print(f"\r[{count}/{len(tasks)}] Current: {task_data[0]:<15} | {file_stats}", end="", flush=True)

    save_files()
    print(f"\nFinal Stats: {get_all_file_counts(args.min_len, args.max_len)}")
    print("Processing complete.")


if __name__ == "__main__":
    main()
