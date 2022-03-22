#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=96m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms256m -Xmx512m -Xss2m"
INNER_JAVA_OPTS="set javaOptions += \"-Dlog4j.configuration=file://$TRAVIS_BUILD_DIR/project/travis-log4j.properties\""

withCmd() {
  CMD=$1
  for t in $TEST_TARGET; do echo -n "; $t/$CMD "; done
}


echo "Running test compilation..."
export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=168m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms512m -Xmx1500m -Xss8m"
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true ++$TRAVIS_SCALA_VERSION "$(withCmd "test:compile")"

echo "Running test suite"
export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=128m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms256m -Xmx768m -Xss2m"
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true ++$TRAVIS_SCALA_VERSION coverage "$(withCmd test)" coverageAggregate
TST_EXIT_CODE=$?

echo "Running mima checks ... "
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true  ++$TRAVIS_SCALA_VERSION "$(withCmd mimaReportBinaryIssues)"
MIMA_EXIT_CODE=$?

echo "Running compile:doc ... "
echo "time ./sbt ++$TRAVIS_SCALA_VERSION $(withCmd compile:doc)"
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true  ++$TRAVIS_SCALA_VERSION "$(withCmd compile:doc)"


echo "all test checks done"
