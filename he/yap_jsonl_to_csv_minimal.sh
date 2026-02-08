#!/bin/bash
# yap_jsonl_to_csv_minimal.sh - POS + lemma only

INPUT_JSONL="${1}"
OUTPUT_CSV="${2:-hebrew_morphology.csv}"

cat << CSV_HEADER > "$OUTPUT_CSV"
lemma,POS,gender,number,person,tense
CSV_HEADER

jq -r '
  (.sentences // empty)[] |
  .tokens[] |
  [
    .lemma,
    .POS,
    (.features // "" | scan("gen=([MF])") | .[0] // ""),
    (.features // "" | scan("num=([SP])") | .[0] // ""),
    (.features // "" | scan("per=([123])") | .[0] // ""),
    (.features // "" | scan("tense=([A-Z]+)") | .[0] // "")
  ] | @csv
' "$INPUT_JSONL" >> "$OUTPUT_CSV"

printf '\xEF\xBB\xBF' | cat - "$OUTPUT_CSV" > temp && mv temp "$OUTPUT_CSV"

echo "✅ Minimal CSV: $(($(wc -l < "$OUTPUT_CSV") - 1)) words"
cat "$OUTPUT_CSV"
