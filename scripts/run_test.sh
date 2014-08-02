# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR
./sbt ++$TRAVIS_SCALA_VERSION $TEST_TARGET/compile $TEST_TARGET/test:compile &> /dev/null

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=96m -XX:+TieredCompilation -XX:MaxPermSize=128m -Xms256m -Xmx512m -Xss2m"

./sbt -Dlog4j.configuration=file://$TRAVIS_BUILD_DIR/project/travis-log4j.properties ++$TRAVIS_SCALA_VERSION $TEST_TARGET/test
