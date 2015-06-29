#!/bin/bash

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR


#Which things do we want to build, all things that start with scalding- or maple are build targets
# This will produce a long string targets separated by spaces
TARGET_NAMES=$(ls -d scalding-* maple)

# Cat the travis build file, ignoring the assembly lines
# Reformatting any quotes to new lines so we get things nicer split up
# Then grep this for the scalding- and maple from above
BUILDS_WE_HAVE=$(cat .travis.yml | grep -v scripts/build_assembly_no_test  | tr '"' '\n' | tr ' ' '\n' | grep -e scalding- -e maple )

# Grab the blacklist, lines starting with #'s
BLACKLIST_BUILDS=$(cat .travis.blacklist | egrep -v '^\s*#')


TEST_ID=$(date '+%s')
GOAL_PATH="/tmp/scalding_goal.$TEST_ID.txt"
HAVE_PATH="/tmp/scalding_gHAVE.$TEST_ID.txt"
# Ideally we want to have each target twice, once for 2.10 and 2.11
# So echo them twice, counting their frequency into the goal path
echo $TARGET_NAMES $TARGET_NAMES | tr ' ' '\n' | sort | uniq -c > $GOAL_PATH

#Now we take the builds we have, appending the
#blacklist builds
echo $BUILDS_WE_HAVE $BLACKLIST_BUILDS | tr ' ' '\n' | sort | uniq -c > $HAVE_PATH

# Once we've done this both lists should be identical
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