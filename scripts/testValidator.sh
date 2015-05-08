#!/bin/bash

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR



TARGET_NAMES=$(ls -d scalding-* maple)

BUILDS_WE_HAVE=$(cat .travis.yml | grep -v scripts/build_assembly_no_test  | tr '"' '\n' | tr ' ' '\n' | grep -e scalding- -e maple )

BLACKLIST_BUILDS=$(cat .travis.blacklist | egrep -v '^\s*#')


TEST_ID=$(date '+%s')
GOAL_PATH="/tmp/scalding_goal.$TEST_ID.txt"
HAVE_PATH="/tmp/scalding_gHAVE.$TEST_ID.txt"
echo $TARGET_NAMES $TARGET_NAMES | tr ' ' '\n' | sort | uniq -c > $GOAL_PATH
echo $BUILDS_WE_HAVE $BLACKLIST_BUILDS | tr ' ' '\n' | sort | uniq -c > $HAVE_PATH

DIFF=$(diff $GOAL_PATH $HAVE_PATH)
RET=$?
rm -f $GOAL_PATH
rm -f $HAVE_PATH

if [ $RET -eq 0 ]; then
    echo "All builds running"
    exit 0
else
    echo -e "Missing some builds, diff $DIFF"
    exit 1
fi