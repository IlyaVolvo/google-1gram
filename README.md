The goal of this effort to create a corpus of words produced typcally by Google Bigquery for a specific language that could be analyzed and cleaned  programmatically or manually. The following query was used to obtain 50,000 words of length from 4 to 7\.  
`SELECT`  
  `n.term,`  
  `y.term_frequency`  
`FROM`  
  `` `bigquery-public-data.google_books_ngrams_2020.<lang>_1` AS n, ``  
  `UNNEST(years) AS y`  
`WHERE`  
 `y.year = 2019`

`-- 1. Filters for the 4 specific tags`  
`-- ^ [a-z] starts with 4 to 7 alpha characters and has underscore for PoS`  
AND (
    REGEXP_CONTAINS(n.term, r'^[[\p{Ll}]{4,7}_NOUN')
    OR REGEXP_CONTAINS(n.term, r'^[\p{Ll}]{4,7}_VERB')
    OR REGEXP_CONTAINS(n.term, r'^[[\p{Ll}]{4,7}_ADV')
    OR REGEXP_CONTAINS(n.term, r'^[[\p{Ll}]{4,7}_ADJ')
`ORDER BY`  
  `y.term_frequency DESC`  
`LIMIT 50000;`  
Where  language could be English, French, Spanish and German:  
  `` `bigquery-public-data.google_books_ngrams_2020.eng_1`, ``  
  `` `bigquery-public-data.google_books_ngrams_2020.fre_1`, ``  
  `` `bigquery-public-data.google_books_ngrams_2020.ger_1`, ``  
  `` `bigquery-public-data.google_books_ngrams_2020.spa_1`, ``  
respectively

# Workflow: Creating Dictionary Candidates for Word Games (Language-Specific)

This document describes a **repeatable, auditable workflow** for producing high-quality word lists suitable for word-based games (Wordle-like, anagrams, spelling games, etc.) for a **specific language**.

The workflow assumes:
- You want **real words**, not generated strings
- You want **control over grammar, morphology, and surface form**
- You want lists that can be **explained, regenerated, and filtered later**

The pipeline is modular: each stage produces an artifact that can be inspected, versioned, and refined.

---

## High-Level Pipeline

```
Wiktionary dump (language-specific)
        ↓
Parse & normalize entries
        ↓
Structured CSV (words + metadata)
        ↓
Rule-based filtering (tags + regex)
        ↓
Candidate lists (by length / POS / rules)
        ↓
Manual review + game-specific tuning
```

---

## 1. Source Data: Wiktionary Locale Dump

### Goal
Obtain **authoritative lexical data** for a single language, including:
- Headwords
- Part of speech
- Morphological / grammatical tags
- (Optionally) frequency or usage notes

### Tool
- `wiktionary_locale_dump.py`

---

## 2. Parsing & Normalization

### Goal
Convert raw Wiktionary data into a **machine-friendly, flat structure** that preserves linguistic meaning.

### Tool
- `wiktionary_parse_results.py`

### Output Format (CSV)

```
word,frequency,part_of_speech,tag1,tag2,tag3,...
```

---

## 3. Canonical Structured Corpus

This CSV is your **single source of truth**.

---

## 4. Rule-Based Filtering

### Tool
- `filter_by_tags.py`

### Example Filters

```
VERB & ![.*er]
NOUN & !PLURAL
(ADJ | ADV) & ![ly$]
```

---

## 5. Length-Specific Candidate Sets

Most word games require fixed-length words (4–7 letters typically).

---

## 6. Game-Specific Constraints

Apply non-linguistic rules:
- Remove archaic / offensive terms
- Balance difficulty
- Remove confusing homographs

---

## 7. Manual Review

Keep a record of removed words and reasons.

---

## 8. Regeneration & Versioning

The entire pipeline is **fully reproducible**.

---

## Guiding Principles

- Transparency over cleverness
- Rules over ad-hoc edits
- Linguistic correctness first
- Everything reproducible
