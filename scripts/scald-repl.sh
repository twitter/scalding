#!/bin/bash

# Identify the base dir in the distribution, and source the common scripts.
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh

${BASE_DIR}/scripts/scald.rb --repl --tool com.twitter.scalding.ScaldingShell $@ "\-Yrepl-sync"

# record the exit status lest it be overwritten:
# then reenable echo and propagate the code.
SCALA_EXIT_STATUS=$?
onExit
