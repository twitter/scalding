#!/bin/bash -exv

# Not sure what the right default is here: trying nonzero.
SCALA_EXIT_STATUS=127
SAVED_STTY=""

SCALD="${BASE_DIR}/scripts/scald.rb --local"
SCALD_REPL="${BASE_DIR}/scripts/scald.rb --repl --local"

echo "using TRAVIS_SCALA_VERSION ${TRAVIS_SCALA_VERSION}"
SCALD="$SCALD --scalaversion ${TRAVIS_SCALA_VERSION}"
SCALD_REPL="$SCALD_REPL --scalaversion ${TRAVIS_SCALA_VERSION}"
