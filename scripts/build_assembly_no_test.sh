#!/bin/bash
set -e # first error should stop execution of this script

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
TARGET=$1

cd $BASE_DIR
sed -i'' -e 's/\/\/ test in assembly/test in assembly/g' project/Build.scala

bash -c "while true; do echo -n .; sleep 5; done" &
PROGRESS_REPORTER_PID=$!

time ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly &> /dev/null

RET_CODE=$?

kill -9 $PROGRESS_REPORTER_PID

if [ $RET_CODE -ne 0 ]; then
  ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly
else
  exit 0
fi
