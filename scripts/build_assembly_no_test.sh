#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
TARGET=$1

cd $BASE_DIR
sed -i'' -e 's/\/\/ test in assembly/test in assembly/g' build.sbt

time ./sbt ++$TRAVIS_SCALA_VERSION $TARGET/assembly
