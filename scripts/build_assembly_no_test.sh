set -e # first error should stop execution of this script

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

cd $BASE_DIR
sed -i'' -e 's/\/\/ test in assembly/test in assembly/g' project/Build.scala
./sbt ++$TRAVIS_SCALA_VERSION assembly
