#!/usr/bin/env python3
"""
extract_words.py

Reads a "validated" CSV produced by the Wiktionary validator and filters/extracts
records by frequency, exact length(s), and a boolean tag expression.

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

FILTERING
---------
Instead of -i/--include and -x/--exclude, use a single expression:

  -x / --expr "<boolean expr>"

Operators (case-insensitive tags):
  &   AND
  |   OR
  ^   NOT   (unary, prefix)
  ( ) grouping

Examples:
  -x noun
  -x "noun | verb"
  -x "noun & ^verb"
"""

import argparse
import sys
from typing import List, Tuple, Optional, Set, Union

Token = Union[str, tuple]  # str for TAG or op tokens, tuple for ('TAG', name)


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
        help='Boolean tag expression using &, |, ^, and parentheses. Example: "noun & ^verb"'
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
    Tokens are: '(', ')', '&', '|', '^', or TAG (alnum/_/-/.)
    Tags are case-insensitive; we normalize to upper later.
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


def eval_rpn(rpn: List[str], tagset_upper: Set[str]) -> bool:
    """
    Evaluate RPN against a set of tags (already normalized to uppercase).
    TAG token is True iff TAG in tagset_upper.
    """
    stack: List[bool] = []

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

        # TAG
        stack.append(tok.upper() in tagset_upper)

    if len(stack) != 1:
        raise ExprError("Invalid expression (leftover operands/operators)")
    return stack[0]


def compile_expr(expr: Optional[str]) -> Optional[List[str]]:
    """
    Compile expression into RPN tokens, or None if no expression provided.
    """
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
        return eval_rpn(rpn_expr, tagset_upper)

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
                    lengths=args.lengths or [],
                    rpn_expr=rpn,
                )
            except ExprError as e:
                print(f"ERROR: expression evaluation failed on word '{word}': {e}", file=sys.stderr)
                sys.exit(2)

            if not ok:
                continue

            if all_tags_ordered:
                fout.write(f"{word}\t{freq}," + ",".join(all_tags_ordered) + "\n")
            else:
                fout.write(f"{word}\t{freq}\n")

            kept += 1

    print(f"Read lines: {total}  Kept: {kept}", file=sys.stderr)


if __name__ == "__main__":
    main()
