#!/bin/bash
# yap_hebrew_batch_final.sh - LINE NUMBER BASED (guaranteed to work)

set -euo pipefail

CSV_FILE="${1}"
BATCH_SIZE="${2:-100}"
START_FROM="${3:-0}"
DN="yap-hebrew"
CONLL_TO_JSON="${4:-conll_to_json.sh}"
OUTPUT_JSONL="${CSV_FILE%.csv}-yap.jsonl"

[[ ! -f "$CSV_FILE" ]] && { echo "ERROR: $CSV_FILE not found"; exit 1; }
[[ "$BATCH_SIZE" -lt 1 ]] && { echo "ERROR: BATCH_SIZE >=1"; exit 1; }
[[ "$START_FROM" -ge 0 ]] || { echo "ERROR: START_FROM >=0"; exit 1; }

# Ensure container
if [[ "$(docker ps --filter name=${DN} --format '{{.Status}}')" != "Up"* ]]; then
    echo "Starting YAP container..."
    docker rm -f ${DN} 2>/dev/null || true
    docker run -d --name ${DN} -p 8000:8000 --memory=8g --platform linux/amd64 onlplab/yap-api
    docker exec -d ${DN} tail -f /dev/null
    sleep 15
fi

echo "Processing $CSV_FILE: batch_size=$BATCH_SIZE, start_from=$START_FROM → $OUTPUT_JSONL"

# Total data lines (skip header)
total_data_lines=$(( $(wc -l < "$CSV_FILE") - 1 ))
words_processed=$START_FROM
batch_count=0

# Process until no more words
while (( words_processed < total_data_lines )); do
    ((batch_count++))

    # Calculate exact line range (CSV line 2+ = data line 1+)
    csv_start_line=$((words_processed + 2))
    csv_end_line=$((words_processed + BATCH_SIZE + 1))

    # Extract EXACTLY these lines
    sed -n "${csv_start_line},${csv_end_line}p" "$CSV_FILE" | \
    cut -d',' -f1 | head -n "$BATCH_SIZE" | \
    grep -v '^$' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' > "batch_${batch_count}.raw"

    batch_word_count=$(wc -l < "batch_${batch_count}.raw")
    [[ "$batch_word_count" -eq 0 ]] && break

    # Format for YAP (word + blank line)
    awk '{print $0; print ""}' "batch_${batch_count}.raw" > "batch_${batch_count}_yap.raw"

    start_word=$((words_processed + 1))
    end_word=$((words_processed + batch_word_count))

    echo "Batch $batch_count: words $start_word-$end_word ($batch_word_count words)"

    # Process
    docker cp "batch_${batch_count}_yap.raw" ${DN}:/tmp/batch.raw
    docker exec ${DN} /yap/src/yap/yap hebma -raw /tmp/batch.raw -out /tmp/batch_lattices.conll
    docker exec ${DN} /yap/src/yap/yap md -in /tmp/batch_lattices.conll -om /tmp/batch_md.conll

    # Append to JSONL with metadata
    {
        echo "{\"batch\":$batch_count,\"words_start\":$start_word,\"words_end\":$end_word,\"word_count\":$batch_word_count,"
        echo "\"total_processed\":$((words_processed + batch_word_count)),\"comment\":\"batch $batch_count\"}"
        docker cp ${DN}:/tmp/batch_md.conll - | "$CONLL_TO_JSON"
    } >> "$OUTPUT_JSONL"

    # Cleanup
    rm "batch_${batch_count}".raw "batch_${batch_count}_yap.raw"

    words_processed=$((words_processed + batch_word_count))

    # Progress
    percent=$((100 * words_processed / total_data_lines ))
    echo "Progress: $percent% ($words_processed/$total_data_lines words)"
done

echo "✅ COMPLETE! $OUTPUT_JSONL ($(wc -l < "$OUTPUT_JSONL") batches)"
echo "Processed: $words_processed words in $batch_c
