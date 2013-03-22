
set -e # first error should stop execution of this script

SCALD="scripts/scald.rb"

$SCALD --local tutorial/Tutorial0.scala
$SCALD --local tutorial/Tutorial1.scala
$SCALD --local tutorial/Tutorial2.scala

$SCALD \
  --local tutorial/Tutorial3.scala \
  --input tutorial/data/hello.txt
 
$SCALD \
  --local tutorial/Tutorial4.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output4.txt
 
$SCALD \
  --local tutorial/Tutorial5.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output5.txt \
  --words tutorial/data/words.txt
 
$SCALD \
  --local tutorial/MatrixTutorial0.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/outdegree.tsv
 
$SCALD \
  --local tutorial/MatrixTutorial1.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cofollows.tsv
 
$SCALD \
  --local tutorial/MatrixTutorial2.scala \
  --input tutorial/data/graph.tsv \
  --maxOutdegree 1000 \
  --output tutorial/data/graphFiltered.tsv
 
$SCALD \
  --local tutorial/MatrixTutorial3.scala \
  --input1 tutorial/data/graph.tsv \
  --input2 tutorial/data/graph2.tsv \
  --intersection tutorial/data/intersection.tsv \
  --leftDiff tutorial/data/leftDiff.tsv \
  --rightDiff tutorial/data/rightDiff.tsv
 
$SCALD \
  --local tutorial/MatrixTutorial5.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cosineSim.tsv
 
