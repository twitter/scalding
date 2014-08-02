#!/bin/bash
set -e # first error should stop execution of this script

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
TARGET=$1

cd $BASE_DIR
sed -i'' -e 's/\/\/ test in assembly/test in assembly/g' project/Build.scala
time ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly &> /dev/null || ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly
