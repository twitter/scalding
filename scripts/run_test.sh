if [ ! -d "~/ivy2custom" ]; then
  echo "downloading ivy cache"
  wget  https://www.dropbox.com/s/ypfu32hzb17sbmz/custom-ivy2.tar.gz?dl=0 -O ~/custom-ivy2.tar.gz
  echo "uncompressing ivy cache"
  tar -zxvf ~/custom-ivy2.tar.gz
  echo "cleaning up"
  rm "~/custom-ivy2.tar.gz"
  echo "done!"
fi

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=96m -XX:+TieredCompilation -XX:MaxPermSize=128m -Xms256m -Xmx512m -Xss2m"

INNER_JAVA_OPTS="set javaOptions += \"-Dlog4j.configuration=file://$TRAVIS_BUILD_DIR/project/travis-log4j.properties\""

withCmd() {
  CMD=$1
  for t in $TEST_TARGET; do echo "; project $t; set logLevel := Level.Warn; $INNER_JAVA_OPTS; ++$TRAVIS_SCALA_VERSION; $CMD"; done
}

bash -c "while true; do echo -n .; sleep 5; done" &

PROGRESS_REPORTER_PID=$!
time ./sbt "$(withCmd "compile; test:compile")" &> /dev/null
kill -9 $PROGRESS_REPORTER_PID

./sbt "$(withCmd test)"
