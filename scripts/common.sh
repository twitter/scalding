#!/bin/bash

# Not sure what the right default is here: trying nonzero.
SCALA_EXIT_STATUS=127
SAVED_STTY=""

# restore stty settings (echo in particular)
function restoreSttySettings() {
  if [[ -n $SCALA_RUNNER_DEBUG ]]; then
    echo "restoring stty: $SAVED_STTY"
  fi

  stty $SAVED_STTY
  SAVED_STTY=""
}

function onExit() {
  if [[ "$SAVED_STTY" != "" ]]; then
    restoreSttySettings
    exit $SCALA_EXIT_STATUS
  fi
}

# to reenable echo if we are interrupted before completing.
trap onExit INT

# save terminal settings
SAVED_STTY=$(stty -g 2>/dev/null)
# clear on error so we don't later try to restore them
if [[ ! $? ]]; then
  SAVED_STTY=""
fi
if [[ -n $SCALA_RUNNER_DEBUG ]]; then
  echo "saved stty: $SAVED_STTY"
fi

SCALD="${BASE_DIR}/scripts/scald.rb --local"
SCALD_REPL="${BASE_DIR}/scripts/scald.rb --repl --local"

# Note: it might be preferable to have .travis.yml pass this as an argument
if [ $TRAVIS_SCALA_VERSION ]; then
  echo "using TRAVIS_SCALA_VERSION ${TRAVIS_SCALA_VERSION}"
  SCALD="$SCALD --scalaversion ${TRAVIS_SCALA_VERSION}"
  SCALD_REPL="$SCALD_REPL --scalaversion ${TRAVIS_SCALA_VERSION}"
fi