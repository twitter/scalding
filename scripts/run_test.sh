# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=96m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms256m -Xmx2048m -Xss2m"

INNER_JAVA_OPTS="set javaOptions += \"-Dlog4j.configuration=file://$TRAVIS_BUILD_DIR/project/travis-log4j.properties\""

withCmd() {
  CMD=$1
  for t in $TEST_TARGET; do
    echo "Running: ./sbt ; project $t; set logLevel := Level.Warn; $INNER_JAVA_OPTS; ++$TRAVIS_SCALA_VERSION; $CMD"
    time ./sbt "; project $t; set logLevel := Level.Warn; $INNER_JAVA_OPTS; ++$TRAVIS_SCALA_VERSION; $CMD" &> /dev/null
  done
}

bash -c "while true; do echo -n .; sleep 5; done" &

PROGRESS_REPORTER_PID=$!
time withCmd "compile; test:compile"
kill -9 $PROGRESS_REPORTER_PID

# Separate JVM's while slower, avoids GC issues in sbt itself
for t in $TEST_TARGET; do
  echo "Running: ./sbt ; project $t; set logLevel := Level.Warn; $INNER_JAVA_OPTS; ++$TRAVIS_SCALA_VERSION; test"
  time ./sbt "; project $t; set logLevel := Level.Warn; $INNER_JAVA_OPTS; ++$TRAVIS_SCALA_VERSION; test" || exit 1
done
