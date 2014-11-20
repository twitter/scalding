set -e # first error should stop execution of this script

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh

# also trap errors, to reenable terminal settings
trap onExit ERR

time $SCALD tutorial/MatrixTutorial0.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/outdegree.tsv

time $SCALD tutorial/MatrixTutorial1.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cofollows.tsv

time $SCALD tutorial/MatrixTutorial2.scala \
  --input tutorial/data/graph.tsv \
  --maxOutdegree 1000 \
  --output tutorial/data/graphFiltered.tsv

time $SCALD tutorial/MatrixTutorial3.scala \
  --input1 tutorial/data/graph.tsv \
  --input2 tutorial/data/graph2.tsv \
  --intersection tutorial/data/intersection.tsv \
  --leftDiff tutorial/data/leftDiff.tsv \
  --rightDiff tutorial/data/rightDiff.tsv

time $SCALD tutorial/MatrixTutorial5.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cosineSim.tsv


# restore stty
SCALA_EXIT_STATUS=0
onExit


