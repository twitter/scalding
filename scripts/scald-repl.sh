#!/bin/bash

# Identify the bin dir in the distribution, and source the common scripts.
bin=`dirname $0`
. ${bin}/common.sh

# Identify the base dir in the distribution.
basedir=`cd ${bin}/.. && pwd`

## find scalding version
SCALDING_VERSION=`cat "${basedir}/version.sbt" |  grep "version in ThisBuild" | grep -Eo "[0-9\.]+(rc)*[0-9\.]+" | head -1`

## find short scala version - use SCALA_VERSION or TRAVIS_SCALA_VERSION if it is set
if [ -z "$SCALA_VERSION" ]; then
  if [ -z "$TRAVIS_SCALA_VERSION" ]; then
    SCALA_VERSION=`cat "${basedir}/project/Build.scala" | grep -E '^\s*scalaVersion' | grep -Eo "[0-9\.]+" | head -1`
  else
    SCALA_VERSION=$TRAVIS_SCALA_VERSION
  fi
fi

## Piggyback off of scald.rb's dependency/cp management
CORE_PATH=`${bin}/scald.rb --scalaversion ${SCALA_VERSION} --print-cp --repl --avro --local job`
if [ $? != 0 ]; then
  echo "scalding-core-assembly jar is missing, you probably need to run sbt assembly"
  exit 1
fi

# figure out mode to decide whether to run using hadoop or not
MODE=""
ARGS=`echo "$@" | tr "[:space:]" "\n"`
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
