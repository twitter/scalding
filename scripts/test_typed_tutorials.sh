#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh

# Test TypedTutorial cases
for t in 1 2 3 4 5 pipes block; do
  echo "--------------------"
  echo "TypedTutorial: $t"
  echo "--------------------"
  time $SCALD tutorial/TypedTutorial.scala \
    --tutorial $t \
    --input tutorial/data/hello.txt \
    --output tutorial/data/output0.txt \
    --words tutorial/data/word_scores.tsv
  echo "--------------------"
  cat tutorial/data/output0.txt
done



