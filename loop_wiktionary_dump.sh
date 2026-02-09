#!/usr/bin/env bash
set -euo pipefail

# Capture all arguments
args=("$@")

# Second argument is the file name
name="${args[1]}"

prev_non=""

while true; do
    echo $(date) Starting processing ${name} ...
    # /opt/homebrew/bin//python3.13 /Users/ilya/project/wordle/google-1gram/cleanse-dictionary.py "${args[@]}"
    /Users/ilya/project/wordle/google-1gram/wiktionary_locale_dump.py "${args[@]}"

    rej="R$name"
    non="N$name"

    #Append non-existing to rejected
    cat $non >>$rej

    if [ ! -s "$rej" ]; then
        echo "$(date) Done — $rej is empty "
        break
    fi

    #if rejected are the same that nonexustent means nothing really changed
    if [ ! -z "$prev_non" ]; then
      if cmp -s <(sort $prev_non) <(sort $non); then
        echo "Nonexistent names didnt change, they likely don't exist!"
        break
      fi
    fi

    echo "$(date) Next iteration $(wc -l $non) words, total $(wc -l $rej)"

    sleep 20
    # Update name for next iteration
    prev_non="$non"
    name="$rej"
    args[1]="$name"
done
