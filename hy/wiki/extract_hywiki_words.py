#!/usr/bin/env python3
"""
Extract Armenian-only words (length 4..7) from hywiki XML dump (xml or xml.bz2),
count frequencies, and optionally attach morphological analyses (morpheme splits)
via an external analyzer command.

Output (CSV by default):
  word,count[,analysis1|analysis2|...]

Notes:
- For best wikitext->plain conversion: pip install mwparserfromhell
- Morph analysis is plugin-based. You provide --analyze-cmd, which will be called
  once per unique word. The command should print 0+ lines of analyses for that word.
"""

import argparse
import bz2
import csv
import io
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Dict, Iterable, List, Optional, Tuple

# Armenian letters (includes 'և' at U+0587)
ARM_WORD_RE = re.compile(r"[Ա-Ֆա-ֆ]+", re.UNICODE)
ARM_EXACT_4_7_RE = re.compile(r"^[Ա-Ֆա-ֆ]{4,7}$", re.UNICODE)

def eprint(*args):
    print(*args, file=sys.stderr)

def open_maybe_bz2(path: str) -> io.BufferedReader:
    # If stdin requested:
    if path == "-":
        # Assume bytes stream
        return sys.stdin.buffer
    # Detect bz2 by extension (works for hywiki-latest-pages-articles.xml.bz2)
    if path.endswith(".bz2"):
        return bz2.open(path, "rb")
    return open(path, "rb")

def strip_wikitext_best_effort(text: str, use_mwparser: bool) -> str:
    """
    Convert MediaWiki wikitext to plain-ish text.
    If mwparserfromhell is available and enabled, use it.
    Otherwise do a conservative regex cleanup.
    """
    if use_mwparser:
        try:
            import mwparserfromhell  # type: ignore
            code = mwparserfromhell.parse(text)
            # remove templates, tags, wikilinks formatting etc. via strip_code
            return code.strip_code(normalize=True, collapse=True)
        except Exception:
            # Fall back
            pass

    # Fallback cleanup (not perfect, but good enough for word extraction):
    # Remove comments
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)
    # Remove refs and other tags
    text = re.sub(r"<ref[^>]*>.*?</ref>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    # Remove templates {{...}}
    text = re.sub(r"\{\{.*?\}\}", " ", text, flags=re.DOTALL)
    # Replace wikilinks [[a|b]] -> b, [[a]] -> a
    text = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", text)
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
    # Remove external links [http://... label]
    text = re.sub(r"\[https?://[^\s\]]+(?:\s+([^\]]+))?\]", r"\1", text)
    # Remove formatting quotes
    text = text.replace("'''", "").replace("''", "")
    return text

def iter_text_nodes(xml_stream: io.BufferedReader) -> Iterable[str]:
    """
    Stream-parse XML and yield each <text> element content.
    Works for very large dumps (iterparse).
    """
    # 'end' events only to keep memory low
    context = ET.iterparse(xml_stream, events=("end",))
    for event, elem in context:
        # MediaWiki dump uses namespaces; elem.tag may look like '{ns}text'
        if elem.tag.endswith("text"):
            if elem.text:
                yield elem.text
            # Important: clear to release memory
            elem.clear()

def extract_words_from_text(text: str, min_len: int, max_len: int) -> Iterable[str]:
    """
    Tokenize Armenian words and keep ONLY tokens of exact length range.
    """
    for tok in ARM_WORD_RE.findall(text):
        # Lowercase using Unicode-aware lower()
        tok = tok.lower()
        L = len(tok)
        if L < min_len or L > max_len:
            continue
        # Ensure token contains ONLY Armenian letters and in range:
        if ARM_EXACT_4_7_RE.match(tok):
            yield tok

def run_analyzer(cmd_template: List[str], word: str, timeout_s: float) -> List[str]:
    """
    Run external analyzer. cmd_template may include "{word}" placeholder in any arg.
    Returns list of non-empty output lines.
    """
    cmd = [arg.replace("{word}", word) for arg in cmd_template]
    try:
        p = subprocess.run(
            cmd,
            input=None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return []
    out_lines = []
    for line in p.stdout.splitlines():
        line = line.strip()
        if line:
            out_lines.append(line)
    return out_lines

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump", help="hywiki-latest-pages-articles.xml or .xml.bz2, or '-' for stdin")
    ap.add_argument("--min", type=int, default=4, help="min word length (default: 4)")
    ap.add_argument("--max", type=int, default=7, help="max word length (default: 7)")
    ap.add_argument("--no-mwparser", action="store_true", help="do not use mwparserfromhell even if installed")
    ap.add_argument("--analyze-cmd", nargs="+", default=None,
                    help="External analyzer command template. Use {word} placeholder. "
                         "Example: --analyze-cmd uniparser-analyze --lang hye --word {word}")
    ap.add_argument("--analyze-timeout", type=float, default=2.0, help="per-word analyzer timeout seconds (default: 2.0)")
    ap.add_argument("--min-count", type=int, default=1, help="only emit words with count >= N (default: 1)")
    ap.add_argument("--top", type=int, default=0, help="emit only top N by frequency (0 = all)")
    ap.add_argument("--tsv", action="store_true", help="output TSV instead of CSV")
    ap.add_argument("--debug-every", type=int, default=0, help="log progress every N text nodes (0 disables)")
    args = ap.parse_args()

    use_mwparser = not args.no_mwparser

    counts = Counter()

    with open_maybe_bz2(args.dump) as f:
        # Wrap as buffered reader if needed
        stream = f if isinstance(f, io.BufferedReader) else io.BufferedReader(f)
        for i, wikitext in enumerate(iter_text_nodes(stream), start=1):
            plain = strip_wikitext_best_effort(wikitext, use_mwparser)
            counts.update(extract_words_from_text(plain, args.min, args.max))
            if args.debug_every and (i % args.debug_every == 0):
                eprint(f"[DEBUG] processed text nodes: {i:,}  unique_words: {len(counts):,}")

    # Filter by min-count
    items = [(w, c) for (w, c) in counts.items() if c >= args.min_count]
    # Sort by frequency desc then alpha
    items.sort(key=lambda x: (-x[1], x[0]))

    if args.top and args.top > 0:
        items = items[: args.top]

    analyses: Dict[str, List[str]] = {}
    if args.analyze_cmd:
        # Analyze each unique word once (expensive; consider --min-count to reduce)
        cmd_template = args.analyze_cmd
        for idx, (w, _) in enumerate(items, start=1):
            analyses[w] = run_analyzer(cmd_template, w, args.analyze_timeout)
            if args.debug_every and (idx % args.debug_every == 0):
                eprint(f"[DEBUG] analyzed words: {idx:,}/{len(items):,}")

    # Output
    dialect = "excel-tab" if args.tsv else "excel"
    writer = csv.writer(sys.stdout, dialect=dialect)

    if args.analyze_cmd:
        writer.writerow(["word", "count", "analyses"])
        for w, c in items:
            # Join multiple analyses with |
            joined = "|".join(analyses.get(w, []))
            writer.writerow([w, c, joined])
    else:
        writer.writerow(["word", "count"])
        for w, c in items:
            writer.writerow([w, c])

if __name__ == "__main__":
    main()
