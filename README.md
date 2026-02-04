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

The further processing is described below, The results of the processing are included  
---

# **Word Validation Pipeline (Wiktionary)**

This repo contains:

1. a **Python validator** that checks words against Wiktionary and classifies them into multiple output CSV artifacts, and

2. a **loop script** that repeatedly reprocesses retryable failures until the process converges (no more hard rejects and nonexistents stabilize).

The goal is to take a large corpus of candidate words (with frequencies) and produce a clean validated list, while isolating different failure modes into separate, inspectable files.

---

## **Input Format**

The validator expects CSV input with at least two columns:

`term,term_frequency`  
`word,12345`

* `term` may include an underscore suffix like `plus_ADV`; the script strips known POS-like suffixes when they match an all-caps tag.

* `term_frequency` must parse as an integer.

* Input is typically sorted by descending frequency (recommended, not strictly required).

### **STDIN mode**

If the input filename is `-`, input is read from standard input. In STDIN mode, validated output goes to STDOUT, and other categories go to STDERR.

---

# **1\) Python validator script**

## **Purpose**

The Python validator:

* queries Wiktionary (REST API) for a word,

* verifies the result is in the requested language/locale section,

* extracts Part-of-Speech (PoS) and additional tags,

* writes one line per word into a validated output file,

* and writes *other outcomes* into separate artifact files so they can be retried or audited.

Wiktionary REST is not perfectly consistent. Transient failures can occur (rate-limits, temporary 404s, backend issues). This pipeline is designed so that:

* validated words are kept,

* retryable cases are isolated,

* nonexistents are tracked separately,

* and hard failures are separated from “wrong locale” cases.

---

## **Invocation**

File mode:

`python cleanse-dictionary.py <lang> <input.csv> [flags...]`

You may omit `.csv` in the input; the script will add it automatically:

`python cleanse-dictionary.py en 50000-words`

STDIN mode:

`cat input.csv | python cleanse-dictionary.py en - > validated.csv`

---

## **Output artifacts (file mode naming)**

For input `foo.csv`, artifacts are written **next to the input**:

* `foo-VALIDATED.csv`  
   Words successfully validated in the requested locale, one output line per word.

* `Nfoo.csv`  
   Words determined to be non-existent (Wiktionary returned 404 after the script’s decision logic). This is not a “hard error”; it is a classification bucket.

* `Rfoo.csv`  
   Hard failures: rate limits, 5xx, network issues (non-exception), other HTTP failures that should be retried later.

* `foo-REJECTED.csv`  
   Rejected bucket: words that were skipped because they do not belong to the requested language section, or cases where one of the required probes failed with an exception (depending on script configuration).

Note: The naming scheme is intentionally chosen so it’s easy to glob for “R\*” and “N\*”.

---

## **Output line format (validated)**

Validated output lines follow:

`<word>,<frequency>,<PoS1>,...,<PoSN>,<additional>`

Where:

* `<word>` is the normalized word (usually the non-capitalized form),

* `<frequency>` is the original numeric frequency,

* `<PoS*>` is a list of distinct part-of-speech labels found in Wiktionary for the requested locale,

* `<additional>` is a semicolon-separated tag string (deduped).

Examples:

`time,3399032,NOUN,SINGULAR`  
`run,123456,NOUN,VERB,FORM_OF;INFLECTED_FORM`

### **Additional tags**

The script currently extracts tags like:

* `SINGULAR` / `PLURAL` (best-effort noun inflection detection)

* verb “inflected form” hints (best-effort) such as `INFLECTED_FORM`, `PAST`, `PAST_PART`, etc.

---

## **Flags**

### **Input / output control**

* `--out`, `-o <file>`  
   Override validated output filename (file mode). If only a basename is provided, the file is placed next to the input.

* `--reset`  
   Delete previously generated artifacts for this input base name before running.

### **Filtering**

* `--min`, `-m <N>`  
   Minimum word length (inclusive). Default: no minimum.

* `--max`, `-M <N>`  
   Maximum word length (inclusive). Default: no maximum.

* `--min-frequency`, `-f <N>`  
   Stop reading input once frequency drops below `N`. This is most effective when input is sorted descending by frequency.

### **Limiting output size**

* `--size`, `-s <N>`  
   Target number of validated outputs. Once `N` validated words are written, processing stops.

### **Concurrency and probing**

* `--workers <N>`  
   Number of concurrent requests to Wiktionary.

* `--cap`  
   Enables a “double probe” mode (non-capitalized and capitalized) and merges the results into a **single validated output line**:

  * probes both forms,

  * merges PoS/additional tags,

  * outputs only one line using the non-capitalized form,

  * moves the word to the retry bucket if one probe fails with an exception.

### **Debugging**

* `--debug`  
   Prints REST URLs, HTTP statuses, and response keys for troubleshooting.

---

## **Processing model**

1. Parse input CSV into tasks: `(word, frequency, raw_line)`.

2. For each task, query Wiktionary REST:

   * default: query the word as-is,

   * with `--cap`: query both lower and capitalized versions and merge.

3. Classification:

   * **Validated**: Wiktionary returns 200 and contains the requested locale section.

   * **Retry**: missing requested locale section OR exception during probes (per config).

   * **Nonexistent**: 404 classification.

   * **Hard rejected**: rate limited, other HTTP failures, unexpected response, etc.

4. Output is buffered and flushed periodically (every 100 completed tasks) for performance.

---

## **Progress output**

Every 100 processed records the script prints a status line (overwriting the previous line), including:

* processed count

* validated / target

* counts for retry / nonexistent / hard rejects

* elapsed time

* average time per processed word

* last word processed

---

# **2\) Loop script**

## **Purpose**

Wiktionary REST can produce transient failures:

* temporary 404s that later succeed,

* rate-limits (429),

* backend hiccups.

The loop script repeatedly reprocesses the **failure buckets** so that transient issues can resolve, while preventing infinite loops by checking convergence.

---

## **High-level process**

1. Run validator on the original input.

2. Build a new “retry input” by combining:

   * hard rejects (`R<base>.csv`)

   * nonexistents (`N<base>.csv`)

   * (optionally) retry bucket (`<base>-REJECTED.csv`) if desired in your workflow

3. Re-run validator on that retry input.

4. Stop when:

   * hard reject output is empty (no more transient failures), AND

   * nonexistents stabilize (the next iteration’s nonexistents are identical to the previous iteration’s nonexistents)

This termination condition ensures you don’t get stuck endlessly retrying the same non-existent words.

---

## **Invocation**

Example:

`./loop_validate.sh python3 cleanse-dictionary.py en 50000-words.csv --workers 8 --min-frequency 1000 --cap`

The loop script passes additional flags through to the Python validator.

---

## **Convergence behavior**

The loop uses two checks:

### **1\) Hard rejects empty**

If the hard reject file is empty, there are no more transient HTTP failures to retry.

### **2\) Nonexistents stable**

Some words will never exist. The loop checks that the newly generated nonexistents file is byte-identical to the previous iteration. If it stops changing, the loop considers this bucket “stable”.

A common implementation uses:

`cmp -s new_nonexistent.csv prev_nonexistent.csv`

---

## **Retry input construction**

The loop script typically creates a retry input file with header:

`term,term_frequency`

and appends terms/frequencies extracted from the failure buckets.

Important notes:

* If rejected files include extra “reason” columns, the loop script should retain only the first two columns (`term,term_frequency`) when producing retry input.

* Deduplication is recommended: if a term appears multiple times, keep the highest frequency.

---

## **Common patterns and usage tips**

### **Ensure Python invocation works inside scripts**

Shell aliases are not available in non-interactive scripts. Use an explicit interpreter:

`python3.10 cleanse-dictionary.py ...`

or use a shebang in the script itself.

### **Compare files ignoring ordering (if needed)**

If nonexistents are not written deterministically (order differs), compare sorted versions:

`cmp <(sort file1) <(sort file2)`

### **Check if a file is empty**

`[ ! -s file.csv ]`

---

# **Known limitations / design notes**

* Wiktionary REST `/page/definition/` is cached and can return transient 404s even for existing pages.

* Locale filtering is strict: if a page exists but doesn’t contain the requested locale key, it will be skipped into the retry bucket.

* Additional tags are “best-effort” and can be improved by deeper parsing of Wiktionary templates; current tags rely on text heuristics.

---

# **Next: Input provenance**

This documentation assumes the input word CSV files already exist and follow the described schema. You can add a section describing where those files come from (Google N-grams, Leipzig corpora, etc.) and how they’re produced/sorted.

# 3\) wtool \- basic processing of loop artifacts.

Combines words, removes duplicates; sorts by frequency

# `extract_words.py` — Boolean Selector for Validated Wiktionary CSV

`extract_words.py` is a **selection and filtering tool**, not a tokenizer.  
It reads a **validated CSV** (typically produced by a Wiktionary validation step) and **selects word records** using:

- **frequency bounds**
- **exact word-length constraints**
- a **boolean expression language** combining:
  - **tags** (metadata attached to each word), and
  - **word-pattern predicates** (glob-style, anchored, case-insensitive)

The script streams its input, evaluates the selection logic per record, and emits only the matching words along with their frequency and tags.

---

## Overview

Typical use cases include:

- Building **answer lists** and **dictionary lists** for word games
- Selecting words by **part of speech**, **quality flags**, or **source tags**
- Excluding words by **spelling shape** (suffixes, length masks, etc.)
- Rapid iteration on selection rules without changing code

---

## Command-line interface

### Synopsis
```bash
python extract_words.py INPUT [-o OUT]
                       [--fmin N] [--fmax N]
                       [-l N ...]
                       [-x EXPR]
