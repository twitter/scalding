#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh

time $SCALD tutorial/Tutorial0.scala
time $SCALD tutorial/Tutorial1.scala
time $SCALD tutorial/Tutorial2.scala

time $SCALD tutorial/Tutorial3.scala \
  --input tutorial/data/hello.txt

time $SCALD tutorial/Tutorial4.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output4.txt

time $SCALD tutorial/Tutorial5.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output5.txt \
  --words tutorial/data/words.txt

time $SCALD tutorial/MatrixTutorial5.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cosineSim.tsv

time $SCALD --json tutorial/JsonTutorial0.scala

time $SCALD --avro --json tutorial/AvroTutorial0.scala


