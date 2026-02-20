#!/usr/bin/env bash
FILE=${1:-/dev/stdin}
sed -E 's/"POS":"([^"]*),"/"POS":"\1","/g' ${FILE} > _cleaned.jsonl
jq -r '.sentences[]?.tokens[]? | select(.features | IN("NN","JJ","VB","BN")) | .POS' _cleaned.jsonl | LC_ALL=he_IL.UTF-8 sort -u
