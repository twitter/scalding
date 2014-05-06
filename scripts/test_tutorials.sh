set -e # first error should stop execution of this script

SCALD="scripts/scald.rb --local"

# Note: it might be preferable to have .travis.yml pass this as an argument
if [ $TRAVIS_SCALA_VERSION ]; then
  echo "using TRAVIS_SCALA_VERSION ${TRAVIS_SCALA_VERSION}"
  SCALD="$SCALD --scalaversion ${TRAVIS_SCALA_VERSION}"
fi

$SCALD tutorial/Tutorial0.scala
$SCALD tutorial/Tutorial1.scala
$SCALD tutorial/Tutorial2.scala

$SCALD tutorial/Tutorial3.scala \
  --input tutorial/data/hello.txt
 
$SCALD tutorial/Tutorial4.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output4.txt
 
$SCALD tutorial/Tutorial5.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output5.txt \
  --words tutorial/data/words.txt
 
$SCALD tutorial/MatrixTutorial0.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/outdegree.tsv
 
$SCALD tutorial/MatrixTutorial1.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cofollows.tsv
 
$SCALD tutorial/MatrixTutorial2.scala \
  --input tutorial/data/graph.tsv \
  --maxOutdegree 1000 \
  --output tutorial/data/graphFiltered.tsv
 
$SCALD tutorial/MatrixTutorial3.scala \
  --input1 tutorial/data/graph.tsv \
  --input2 tutorial/data/graph2.tsv \
  --intersection tutorial/data/intersection.tsv \
  --leftDiff tutorial/data/leftDiff.tsv \
  --rightDiff tutorial/data/rightDiff.tsv
 
$SCALD tutorial/MatrixTutorial5.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cosineSim.tsv

$SCALD --json tutorial/JsonTutorial0.scala

$SCALD --avro --json tutorial/AvroTutorial0.scala
