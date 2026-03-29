#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Design: Aram
Coding: Perplexity
Date: 2026-03-28
"""

DEBUG_WORD: str = ""
DEBUG_FILE: str = "debug.txt"


def _debug_log(label: str, text: str) -> None:
    if not DEBUG_WORD:
        return
    try:
        with open(DEBUG_FILE, "a", encoding="utf-8") as f:
            f.write(f"=== {label} ===\n")
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
            f.write("\n")
    except Exception:
        # Debugging must never break normal processing
        pass
