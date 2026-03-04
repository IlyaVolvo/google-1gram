#!/usr/bin/env bash

# Usage: split-list.bash <name>.csv n

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <name>.csv n" >&2
  exit 1
fi

infile=$1
n=$2

if [ ! -f "$infile" ]; then
  echo "Error: input file '$infile' not found" >&2
  exit 1
fi

# Basic check that n is a positive integer
case "$n" in
  ''|*[!0-9]*)
    echo "Error: n must be a positive integer" >&2
    exit 1
    ;;
esac

if [ "$n" -le 0 ]; then
  echo "Error: n must be a positive integer" >&2
  exit 1
fi

# Strip directory and extension to get <name>
base=$(basename -- "$infile")          # e.g. words.csv -> words.csv[web:6][web:9]
name=${base%.*}                        # e.g. words.csv -> words

more_file="${name}-more-${n}.csv"
less_file="${name}-less-${n}.csv"

# Split based on 2nd (count) column (comma-separated, numeric compare) [web:13]
awk -F',' -v n="$n" -v OFS=',' '
  {
    # $1 = word, $2 = count (assumed positive integer), no header
    if ($2 >= n) {
      print > more
    } else {
      print > less
    }
  }
' more="$more_file" less="$less_file" "$infile"
