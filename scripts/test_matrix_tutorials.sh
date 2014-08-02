set -e # first error should stop execution of this script

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh

# also trap errors, to reenable terminal settings
trap onExit ERR

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

$SCALD --json tutorial/JsonTutorial0.scala

$SCALD --avro --json tutorial/AvroTutorial0.scala

# restore stty
SCALA_EXIT_STATUS=0
onExit


