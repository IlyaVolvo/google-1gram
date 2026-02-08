awk '
BEGIN {
  print "{\"sentences\":[";
  first_sentence = 1
}
NR==1 { next }  # Skip headers
$0 ~ /^$/ || NF==0 {  # Blank line = end of sentence
  if (tokens > 0) {
    if (!first_sentence) printf ","  # Comma BEFORE next sentence (not after)
    first_sentence = 0

    printf "{\"sentence_id\":%d,\"tokens\":[\n", sentence_id
    for(i=1; i<=tokens; i++) {
      if (i > 1) printf ","  # Comma before 2nd+ token
      printf "  {\"id\":%d,\"form\":\"%s\",\"lemma\":\"%s\",\"POS\":\"%s\",\"features\":\"%s\"}\n",
             ids[i], forms[i], lemmas[i], poss[i], feats[i]
    }
    printf "]}\n"
    sentence_id++
  }
  tokens=0
  next
}
{
  if (NF >= 5) {  # Valid CoNLL line
    tokens++
    ids[tokens]=$1;
    forms[tokens]=$2;
    lemmas[tokens]=$3;
    poss[tokens]=$4;
    feats[tokens]=$5
  }
}
END {
  print "]}"
}' $1
