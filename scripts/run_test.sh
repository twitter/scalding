#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR

withCmd() {
  CMD=$1
  for t in $TEST_TARGET; do echo -n "; $t/$CMD "; done
}

# collecting code coverage has a fairly high memory pressure
export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=128m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms512m -Xmx2g -Xss8m"

echo "Running test compilation..."
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true ++$TRAVIS_SCALA_VERSION "$(withCmd "test:compile")"

echo "Running test suite"
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true ++$TRAVIS_SCALA_VERSION "$(withCmd test)" jacocoReport
TST_EXIT_CODE=$?

echo "Running mima checks ... "
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true  ++$TRAVIS_SCALA_VERSION "$(withCmd mimaReportBinaryIssues)"

echo "Running compile:doc ... "
echo "time ./sbt ++$TRAVIS_SCALA_VERSION $(withCmd compile:doc)"
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true  ++$TRAVIS_SCALA_VERSION "$(withCmd compile:doc)"


echo "all test checks done"
