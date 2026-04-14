#!/usr/bin/env python3
import unicodedata
import sys
import contextlib

INPUT = sys.argv[1]
LENGTH = int (sys.argv[2])

with contextlib.ExitStack() as stack:
    if INPUT == "-":
        f = sys.stdin
    else:
        f = stack.enter_context(open(INPUT, "r", encoding="utf-8"))

    for line in f:
        word = line.strip()

        # Normalize and remove combining marks (niqqud / cantillation)
        word = unicodedata.normalize("NFC", word)
        word = "".join(
            ch for ch in word
            if unicodedata.category(ch) != "Mn"
        )

        # Keep only Hebrew letters א–ת
        if all('\u05D0' <= ch <= '\u05EA' for ch in word) and len(word) == LENGTH:
            print(word)
