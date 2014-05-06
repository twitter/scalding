#!/bin/bash

function show_help() {
    echo "USAGE: $0 [-h] [-x local|hdfs]"
    echo "   -h                   Show this help message and exit"
    echo "   -x local|hdfs        Run REPL in local (default) or hdfs mode"
}

# command-line argument handling
MODE="local"
while getopts "hx:" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    x)  MODE=$OPTARG;
        ;;
    esac
done
if [ "$MODE" != "local" ] && [ "$MODE" != "hdfs" ]; then
  show_help
  exit 1
fi

# Identify the bin dir in the distribution from which this script is running.
bin=`dirname $0`
bin=`cd ${bin}/.. && pwd`

# Not sure what the right default is here: trying nonzero.
scala_exit_status=127
saved_stty=""

# restore stty settings (echo in particular)
function restoreSttySettings() {
  if [[ -n $SCALA_RUNNER_DEBUG ]]; then
    echo "restoring stty: $saved_stty"
  fi

  stty $saved_stty
  saved_stty=""
}

function onExit() {
  if [[ "$saved_stty" != "" ]]; then
    restoreSttySettings
    exit $scala_exit_status
  fi
}

# to reenable echo if we are interrupted before completing.
trap onExit INT

# save terminal settings
saved_stty=$(stty -g 2>/dev/null)
# clear on error so we don't later try to restore them
if [[ ! $? ]]; then
  saved_stty=""
fi
if [[ -n $SCALA_RUNNER_DEBUG ]]; then
  echo "saved stty: $saved_stty"
fi

## find scalding version
SCALDING_VERSION=`cat "${bin}/version.sbt" |  grep "version in ThisBuild" | grep -Eo "[0-9\.]+(rc)*[0-9\.]+" | head -1`

## find short scala version
SCALA_VERSION=`cat "${bin}/project/Build.scala" | grep -E '^\s*scalaVersion' | grep -Eo "[0-9\.]+" | head -1`

## Piggyback off of scald.rb's dependency/cp management
CORE_PATH=`${bin}/scripts/scald.rb --print-cp --repl --avro --local job`

# launch REPL
if [[ "$MODE" == "local" ]]; then
  java -cp "${CORE_PATH}" -Dscala.usejavacp=true com.twitter.scalding.ScaldingShell -Yrepl-sync --local
else
  REPL_JAR=`echo ${CORE_PATH} | tr ':' '\n' | grep repl`
  HADOOP_CLASSPATH=${CORE_PATH} hadoop jar $REPL_JAR -usejavacp --hdfs
fi

# record the exit status lest it be overwritten:
# then reenable echo and propagate the code.
scala_exit_status=$?
onExit
