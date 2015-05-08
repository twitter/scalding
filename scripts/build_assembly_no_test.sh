#!/bin/bash
set -e # first error should stop execution of this script
# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
TARGET=scalding-$1

cd $BASE_DIR
sed -i'' -e 's/\/\/ test in assembly/test in assembly/g' project/Build.scala

bash -c "while true; do echo -n .; sleep 5; done" &
PROGRESS_REPORTER_PID=$!
# Turn off exit on error while we run this command that we allow fail
set +e
time ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly &> /dev/null

RET_CODE=$?
# Turn back on
set -e

kill $PROGRESS_REPORTER_PID >& /dev/null

if [ $RET_CODE -ne 0 ]; then
  ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly
else
  exit 0
fi
