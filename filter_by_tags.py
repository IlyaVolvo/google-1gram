#!/usr/bin/env python3
"""
filter_by_tags.py

Filter a tagged CSV (like the output of wiktionary_parse_results.py) using a boolean expression over tags
AND (optionally) a regular expression match against the word.

Input CSV format:
  word,frequency,part_of_speech,tag1,tag2,tag3,...

Tags are separate CSV columns (not comma/semicolon lists).

Usage:
  ./filter_by_tags.py input.csv 'PLURAL & NON_NOMINATIVE'
  ./filter_by_tags.py - 'VERB & (INFINITIVE | PAST)' < input.csv > out.csv
  ./filter_by_tags.py input.csv 'VERB & ![.*er]'   # regex on word column

Expression language:
  - TAG names: [A-Z0-9_]+
  - REGEX literal: [ ... ]     (distinguished from tags by square brackets)
      * Matches the word column (row[0]) using re.search()
      * Example: VERB & ![.*er]
  - Operators:
      !   NOT
      &   AND
      |   OR
  - Parentheses: ( ... )
  - Whitespace ignored.

Evaluation:
  - The expression is evaluated against the tag set of each row.
  - part_of_speech is also added to the tag set (e.g., NOUN/VERB/ADJ/ADV),
    so you can filter like: 'NOUN & PLURAL'
"""

from __future__ import annotations

import csv
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple


# Token types:
#   - regex literal:  [ ... ]   (no nesting; ends at first ])
#   - tag:            [A-Z0-9_]+
#   - operators/parens: ( ) ! & |
TOKEN_RE = re.compile(r"\s*(\[[^\]]+\]|[A-Z0-9_]+|[()!&|])\s*")


class ParseError(Exception):
    pass


# -------------------- Expression AST --------------------

@dataclass(frozen=True)
class Node:
    pass


@dataclass(frozen=True)
class Tag(Node):
    name: str


@dataclass(frozen=True)
class Regex(Node):
    pattern: str
    compiled: re.Pattern


@dataclass(frozen=True)
class Not(Node):
    child: Node


@dataclass(frozen=True)
class And(Node):
    left: Node
    right: Node


@dataclass(frozen=True)
class Or(Node):
    left: Node
    right: Node


# -------------------- Tokenizer --------------------

def tokenize(expr: str) -> List[str]:
    tokens: List[str] = []
    i = 0
    while i < len(expr):
        m = TOKEN_RE.match(expr, i)
        if not m:
            snippet = expr[i:i + 20]
            raise ParseError(f"Unexpected token near: {snippet!r}")
        tok = m.group(1)
        tokens.append(tok)
        i = m.end()
    return tokens


# -------------------- Recursive descent parser --------------------
# Precedence:
#   ! highest
#   & mid
#   | lowest

class Parser:
    def __init__(self, tokens: List[str]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Optional[str]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def eat(self, expected: Optional[str] = None) -> str:
        tok = self.peek()
        if tok is None:
            raise ParseError("Unexpected end of expression")
        if expected is not None and tok != expected:
            raise ParseError(f"Expected {expected!r}, got {tok!r}")
        self.pos += 1
        return tok

    def parse(self) -> Node:
        node = self.parse_or()
        if self.peek() is not None:
            raise ParseError(f"Unexpected trailing token: {self.peek()!r}")
        return node

    def parse_or(self) -> Node:
        node = self.parse_and()
        while self.peek() == "|":
            self.eat("|")
            rhs = self.parse_and()
            node = Or(node, rhs)
        return node

    def parse_and(self) -> Node:
        node = self.parse_unary()
        while self.peek() == "&":
            self.eat("&")
            rhs = self.parse_unary()
            node = And(node, rhs)
        return node

    def parse_unary(self) -> Node:
        tok = self.peek()
        if tok == "!":
            self.eat("!")
            return Not(self.parse_unary())
        return self.parse_primary()

    def parse_primary(self) -> Node:
        tok = self.peek()
        if tok is None:
            raise ParseError("Unexpected end of expression")

        if tok == "(":
            self.eat("(")
            node = self.parse_or()
            self.eat(")")
            return node

        if tok.startswith("[") and tok.endswith("]"):
            self.eat()
            pattern = tok[1:-1]
            try:
                compiled = re.compile(pattern)
            except re.error as e:
                raise ParseError(f"Invalid regex [{pattern}]: {e}")
            return Regex(pattern=pattern, compiled=compiled)

        if re.fullmatch(r"[A-Z0-9_]+", tok):
            self.eat()
            return Tag(tok)

        raise ParseError(f"Unexpected token: {tok!r}")


def compile_expr(expr: str) -> Node:
    tokens = tokenize(expr)
    if not tokens:
        raise ParseError("Empty expression")
    return Parser(tokens).parse()


# -------------------- Evaluator --------------------

def eval_node(node: Node, tags: Set[str], word: str) -> bool:
    if isinstance(node, Tag):
        return node.name in tags
    if isinstance(node, Regex):
        return bool(node.compiled.search(word))
    if isinstance(node, Not):
        return not eval_node(node.child, tags, word)
    if isinstance(node, And):
        return eval_node(node.left, tags, word) and eval_node(node.right, tags, word)
    if isinstance(node, Or):
        return eval_node(node.left, tags, word) or eval_node(node.right, tags, word)
    raise TypeError(f"Unknown node type: {type(node)}")


# -------------------- CSV handling --------------------

def read_rows(fin) -> Tuple[List[str], List[List[str]]]:
    r = csv.reader(fin)
    header = next(r, None)
    if header is None:
        return [], []
    rows = [row for row in r]
    return header, rows


def row_tagset(row: List[str]) -> Set[str]:
    # Expected: word,frequency,part_of_speech,tag1,tag2,...
    tags: Set[str] = set()
    if len(row) >= 3:
        pos = (row[2] or "").strip().upper()
        if pos:
            tags.add(pos)  # allow expression to include POS (e.g. NOUN)
    for t in row[3:]:
        t = (t or "").strip().upper()
        if t:
            tags.add(t)
    return tags


# -------------------- Main --------------------

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: filter_by_tags.py <input.csv | -> '<expr>'", file=sys.stderr)
        sys.exit(2)

    src = sys.argv[1]
    expr = sys.argv[2]

    try:
        ast = compile_expr(expr)
    except ParseError as e:
        print(f"ERROR: bad expression: {e}", file=sys.stderr)
        sys.exit(2)

    if src == "-":
        fin = sys.stdin
    else:
        if not os.path.exists(src):
            print(f"ERROR: input not found: {src}", file=sys.stderr)
            sys.exit(2)
        fin = open(src, "r", encoding="utf-8", newline="")

    try:
        reader = csv.reader(fin)
        writer = csv.writer(sys.stdout)

        header = next(reader, None)
        if header is None:
            return
        writer.writerow(header)

        for row in reader:
            if not row:
                continue
            word = (row[0] if len(row) >= 1 else "").strip()
            tags = row_tagset(row)
            if eval_node(ast, tags, word):
                writer.writerow(row)

    finally:
        if src != "-":
            try:
                fin.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
