set -e # first error should stop execution of this script

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh
SHORT_SCALA_VERSION=${TRAVIS_SCALA_VERSION%.*}
SCALDING_VERSION=`cat ${BASE_DIR}/version.sbt`
SCALDING_VERSION=${SCALDING_VERSION#*\"}
SCALDING_VERSION=${SCALDING_VERSION%\"}


# also trap errors, to reenable terminal settings
trap onExit ERR
export CLASSPATH=tutorial/execution-tutorial/target/scala-${SHORT_SCALA_VERSION}/execution-tutorial-assembly-${SCALDING_VERSION}.jar
time java -jar tutorial/execution-tutorial/target/scala-${SHORT_SCALA_VERSION}/execution-tutorial-assembly-${SCALDING_VERSION}.jar \
    com.twitter.scalding.tutorial.MyExecJob --local \
    --input tutorial/data/hello.txt \
    --output tutorial/data/execution_output.txt

# restore stty
SCALA_EXIT_STATUS=0
onExit


