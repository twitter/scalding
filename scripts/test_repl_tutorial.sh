#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
source ${BASE_DIR}/scripts/common.sh

# Now run a basic test for the REPL
# If the content of the output is different, diff will fail with a non-zero exit code
time $SCALD_REPL < tutorial/ReplTutorial1.scala
diff tutorial/data/hello.txt tutorial/data/output1.txt

# Run from inside tutorial directory so we pick up definition
# of 'scaldingReplInitWasLoaded' from 'tutorial/.scalding_repl'
# If it does, then this 'script' exits early with success.
# Otherwise it continues and exits with an error.
cd tutorial; echo "
if (scaldingReplInitWasLoaded) System.exit(0)
System.exit(1)
" | $SCALD_REPL

# Test running repl from sbt.
cd $BASE_DIR
# We need to clean SBT_OPTS, because on travis default SBT_OPTS enable sbt batch mode, which finishes repl process immediately before passing `System.exit`.
echo 'System.exit(0)' | SBT_OPTS='' ./sbt ++$TRAVIS_SCALA_VERSION 'scalding-repl/run --local'
