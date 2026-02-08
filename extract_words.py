#!/usr/bin/env python3
"""
extract_words.py

Reads a "validated" CSV produced by the Wiktionary validator and filters/extracts
records by frequency, exact length(s), and a boolean expression over tags AND
word-pattern tokens.

INPUT (one record per line; original information preserved as tags):
  <word>,<frequency>,<PoS1>,...,<PoSN>,<additional>

Tags:
  - PoS tokens: columns [2:-1] (if present)
  - additional tags: last column split by ';' (if present)
All tags are treated case-insensitively.

OUTPUT FORMAT
-------------
  <word>\t<frequency>,<all tags>
Where <all tags> is:
  PoS tokens (original order), then additional tokens (original order), deduped by first occurrence.

FILTERING EXPRESSION
--------------------
Use:
  -x / --expr "<boolean expr>"

Operators:
  &   AND
  |   OR
  ^   NOT   (unary, prefix)
  ( ) grouping

Operands:
  1) TAG token (case-insensitive): e.g. noun, VERB, SINGULAR
     True iff that tag is present in the record's tag set.

  2) PATTERN token in brackets: [ ... ]
     True iff the WORD matches the pattern.

     Example:
       [*ed]        matches words ending in "ed"
       ^[*ed]       excludes words ending in "ed"
       noun & ^[*ed]

Pattern semantics:
  - We treat bracket contents as a simple wildcard pattern:
      *  matches any sequence (like glob)
      ?  matches any single character
    Everything else is treated literally.
  - Matching is case-insensitive.
  - The pattern must match the entire word (anchored).

Examples:
  -x noun
  -x "noun | verb"
  -x "noun & ^verb"
  -x "[*ed]"
  -x "noun & ^[*ed]"
"""

import argparse
import sys
import re
from typing import List, Tuple, Optional, Set


# -------------------- CLI --------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("input", help="Validated CSV input file, or '-' for stdin")
    p.add_argument("-o", "--out", default="-", help="Output file, or '-' for stdout (default: '-')")

    p.add_argument("--fmin", type=int, default=None, help="Minimum frequency (inclusive)")
    p.add_argument("--fmax", type=int, default=None, help="Maximum frequency (inclusive)")

    p.add_argument(
        "-l", "--len",
        dest="lengths",
        type=int,
        action="append",
        default=[],
        help="Only keep words of this exact length (may be repeated). If omitted, any length allowed."
    )

    p.add_argument(
        "-x", "--expr",
        dest="expr",
        default=None,
        help='Boolean expression using &, |, ^, (), TAGS and [PATTERNS]. Example: "noun & ^[*ed]"'
    )

    return p.parse_args()


def open_in(path: str):
    return sys.stdin if path == "-" else open(path, "r", encoding="utf-8")


def open_out(path: str):
    return sys.stdout if path == "-" else open(path, "w", encoding="utf-8")


# -------------------- CSV parsing / tag preservation --------------------

def _ordered_unique(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def parse_validated_csv_line_preserve(line: str) -> Optional[Tuple[str, int, List[str], List[str]]]:
    """
    Returns (word, freq, pos_tags_in_order, additional_tags_in_order)
    """
    line = line.strip()
    if not line:
        return None

    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 2:
        return None

    word = parts[0]
    try:
        freq = int(parts[1])
    except Exception:
        return None

    pos_tags: List[str] = []
    add_tags: List[str] = []

    if len(parts) >= 4:
        pos_tags = [p for p in parts[2:-1] if p]
        last = parts[-1]
        if last:
            add_tags = [t.strip() for t in last.split(";") if t.strip()]
    elif len(parts) == 3:
        third = parts[2]
        if third:
            if ";" in third:
                add_tags = [t.strip() for t in third.split(";") if t.strip()]
            else:
                add_tags = [third.strip()]

    return word, freq, pos_tags, add_tags


# -------------------- Expression parsing --------------------

class ExprError(ValueError):
    pass


def tokenize_expr(expr: str) -> List[str]:
    """
    Tokens are:
      - '(', ')', '&', '|', '^'
      - TAG token (alnum/_/-/.)
      - PATTERN token in brackets: [ ... ]  (no nesting)

    Example: "noun & ^[*ed]" -> ["noun","&","^","[*ed]"]
    """
    expr = expr.strip()
    if not expr:
        return []

    tokens: List[str] = []
    i = 0
    n = len(expr)

    def is_tag_char(ch: str) -> bool:
        return ch.isalnum() or ch in "_-./"

    while i < n:
        ch = expr[i]

        if ch.isspace():
            i += 1
            continue

        if ch in "()&|^":
            tokens.append(ch)
            i += 1
            continue

        # bracket pattern token: [ ... ]
        if ch == "[":
            j = i + 1
            while j < n and expr[j] != "]":
                j += 1
            if j >= n or expr[j] != "]":
                raise ExprError("Unclosed '[' in expression")
            tokens.append(expr[i:j + 1])  # include brackets
            i = j + 1
            continue

        # normal TAG token
        if is_tag_char(ch):
            j = i + 1
            while j < n and is_tag_char(expr[j]):
                j += 1
            tokens.append(expr[i:j])
            i = j
            continue

        raise ExprError(f"Invalid character in expression: {ch!r}")

    return tokens


def to_rpn(tokens: List[str]) -> List[str]:
    """
    Shunting-yard to convert to Reverse Polish Notation.
    Operators:
      ^ (NOT) unary, highest precedence, right-associative
      & (AND) next
      | (OR) lowest
    """
    prec = {"^": 3, "&": 2, "|": 1}
    right_assoc = {"^"}

    output: List[str] = []
    stack: List[str] = []

    for tok in tokens:
        if tok not in ("^", "&", "|", "(", ")"):
            output.append(tok)
            continue

        if tok in ("^", "&", "|"):
            while stack:
                top = stack[-1]
                if top == "(":
                    break
                if (prec[top] > prec[tok]) or (prec[top] == prec[tok] and tok not in right_assoc):
                    output.append(stack.pop())
                else:
                    break
            stack.append(tok)
            continue

        if tok == "(":
            stack.append(tok)
            continue

        if tok == ")":
            while stack and stack[-1] != "(":
                output.append(stack.pop())
            if not stack or stack[-1] != "(":
                raise ExprError("Mismatched parentheses")
            stack.pop()
            continue

    while stack:
        if stack[-1] in ("(", ")"):
            raise ExprError("Mismatched parentheses")
        output.append(stack.pop())

    return output


def glob_to_regex(pat: str) -> re.Pattern:
    """
    Convert a simple wildcard pattern to a compiled regex:
      * -> .*   ? -> .
    Everything else is literal.
    Anchored to whole word.
    Case-insensitive.
    """
    # Escape everything, then unescape our wildcards.
    # We do this by building manually.
    out = ["^"]
    for ch in pat:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        else:
            out.append(re.escape(ch))
    out.append("$")
    return re.compile("".join(out), re.IGNORECASE)


def eval_token(tok: str, word: str, tagset_upper: Set[str], pat_cache: dict) -> bool:
    """
    Evaluate an operand token:
      - TAG: True iff tag in tagset_upper
      - [pattern]: True iff word matches pattern (case-insensitive)
    """
    if tok.startswith("[") and tok.endswith("]"):
        inner = tok[1:-1]
        if inner == "":
            raise ExprError("Empty [] pattern token is not allowed")
        # cache compiled patterns
        rx = pat_cache.get(inner)
        if rx is None:
            rx = glob_to_regex(inner)
            pat_cache[inner] = rx
        return rx.match(word) is not None

    return tok.upper() in tagset_upper


def eval_rpn(rpn: List[str], word: str, tagset_upper: Set[str]) -> bool:
    """
    Evaluate RPN against:
      - tag set (upper)
      - word string (for [pattern] tokens)
    """
    stack: List[bool] = []
    pat_cache: dict = {}

    for tok in rpn:
        if tok == "^":
            if not stack:
                raise ExprError("NOT (^) missing operand")
            stack.append(not stack.pop())
            continue

        if tok in ("&", "|"):
            if len(stack) < 2:
                raise ExprError(f"Operator {tok} missing operand(s)")
            b = stack.pop()
            a = stack.pop()
            stack.append(a and b if tok == "&" else a or b)
            continue

        # operand
        stack.append(eval_token(tok, word, tagset_upper, pat_cache))

    if len(stack) != 1:
        raise ExprError("Invalid expression (leftover operands/operators)")
    return stack[0]


def compile_expr(expr: Optional[str]) -> Optional[List[str]]:
    if not expr:
        return None
    toks = tokenize_expr(expr)
    if not toks:
        return None
    return to_rpn(toks)


# -------------------- Filtering --------------------

def passes_filters(
    word: str,
    freq: int,
    tagset_upper: Set[str],
    fmin: Optional[int],
    fmax: Optional[int],
    lengths: List[int],
    rpn_expr: Optional[List[str]],
) -> bool:
    if fmin is not None and freq < fmin:
        return False
    if fmax is not None and freq > fmax:
        return False
    if lengths and len(word) not in set(lengths):
        return False

    if rpn_expr is not None:
        return eval_rpn(rpn_expr, word, tagset_upper)

    return True


# -------------------- Main --------------------

def main() -> None:
    args = parse_args()

    try:
        rpn = compile_expr(args.expr)
    except ExprError as e:
        print(f"ERROR: invalid --expr: {e}", file=sys.stderr)
        sys.exit(2)

    total = 0
    kept = 0

    lengths = args.lengths or []

    with open_in(args.input) as fin, open_out(args.out) as fout:
        for raw in fin:
            total += 1
            line = raw.strip()
            if not line:
                continue

            low = line.lower()
            if low.startswith("term,") or low.startswith("word,") or "term_frequency" in low:
                continue

            parsed = parse_validated_csv_line_preserve(line)
            if not parsed:
                continue

            word, freq, pos_tags, add_tags = parsed

            all_tags_ordered = _ordered_unique(pos_tags + add_tags)
            tagset_upper = {t.upper() for t in all_tags_ordered}

            try:
                ok = passes_filters(
                    word=word,
                    freq=freq,
                    tagset_upper=tagset_upper,
                    fmin=args.fmin,
                    fmax=args.fmax,
                    lengths=lengths,
                    rpn_expr=rpn,
                )
            except ExprError as e:
                print(f"ERROR: expression evaluation failed on word '{word}': {e}", file=sys.stderr)
                sys.exit(2)

            if not ok:
                continue

            # Output: <word>\t<frequency>,<all tags>
            if all_tags_ordered:
                fout.write(f"{word},{freq}," + ",".join(all_tags_ordered) + "\n")
            else:
                fout.write(f"{word},{freq}\n")

            kept += 1

    print(f"Read lines: {total}  Kept: {kept}", file=sys.stderr)


if __name__ == "__main__":
    main()
