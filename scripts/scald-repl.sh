#!/bin/bash

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
if [ $? != 0 ]; then
  echo "scalding-core-assembly jar is missing, you probably need to run sbt assembly"
  exit 1
fi

# figure out mode to decide whether to run using hadoop or not
MODE=""
ARGS=$(echo "$@" | tr "[:space:]" "\n")
for a in $ARGS;
do
  if [[ "$a" == "--local" ]] || [[ "$a" == "-local" ]]; then
    MODE="local"; break;
  elif [[ "$a" == "--hdfs" ]] || [[ "$a" == "-hdfs" ]]; then
    MODE="hdfs"; break;
  fi
done

# launch REPL
if [[ "$MODE" == "local" ]]; then
  java -cp "${CORE_PATH}" -Dscala.usejavacp=true com.twitter.scalding.ScaldingShell "$@" -Yrepl-sync
elif [[ "$MODE" == "hdfs" ]]; then
  # get the path for the REPL jar
  REPL_JAR=`echo ${CORE_PATH} | tr ':' '\n' | grep scalding-repl`
  if [ -z "$REPL_JAR" ]; then
      echo "scalding-repl-assembly jar is missing, confirm that it is being built by sbt assembly"
      exit 1
  fi
  HADOOP_CLASSPATH=${CORE_PATH} hadoop jar $REPL_JAR "$@" -usejavacp
else
  echo "Mode must be one of --local or --hdfs, you provided neither"
  exit 1
fi

# record the exit status lest it be overwritten:
# then reenable echo and propagate the code.
scala_exit_status=$?
onExit
