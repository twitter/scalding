# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=96m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms256m -Xmx512m -Xss2m"
$BASE_DIR/scripts/unpackDeps.sh


INNER_JAVA_OPTS="set javaOptions += \"-Dlog4j.configuration=file://$TRAVIS_BUILD_DIR/project/travis-log4j.properties\""

withCmd() {
  CMD=$1
  for t in $TEST_TARGET; do echo -n "; $t/$CMD "; done
}

bash -c "while true; do echo -n .; sleep 5; done" &

PROGRESS_REPORTER_PID=$!

echo "running..."

echo time $TRAVIS_WAIT ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=false ++$TRAVIS_SCALA_VERSION "$(withCmd "test:compile")"
export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=168m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms512m -Xmx1500m -Xss8m"

time $TRAVIS_WAIT ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=false ++$TRAVIS_SCALA_VERSION "$(withCmd "test:compile")" # &> /dev/null

kill -9 $PROGRESS_REPORTER_PID

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=128m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms256m -Xmx768m -Xss2m"
echo "calling ... "
echo "time ./sbt ++$TRAVIS_SCALA_VERSION $(withCmd test)"
time $TRAVIS_WAIT ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=false  ++$TRAVIS_SCALA_VERSION "$(withCmd test)"
TST_EXIT_CODE=$?
echo "all done"


$BASE_DIR/scripts/packDeps.sh
exit $TST_EXIT_CODE
