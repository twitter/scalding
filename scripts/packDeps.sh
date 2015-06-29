echo "Packing environment after success... this make take ~10mins or more"
bash -c "while true; do echo -n .; sleep 5; done" &

ORIG_PATH=`pwd`

PROGRESS_REPORTER_PID=$!
DEP_CACHE_PATH=$HOME/.scalding.dep.cache
mkdir -p $DEP_CACHE_PATH
ID=$(echo $TEST_TARGET | md5deep)
# cd $ORIG_PATH
# tar zcf ~/.ivy2/cache/scalding-cache.tar.gz project/target

cd ~/.ivy2

rm -f cache/*.properties cache/ivy-report*
find cache -name '*.jar' -exec rm -f {} \;
tar cf $DEP_CACHE_PATH/cache-new.tar cache

cd $DEP_CACHE_PATH

if [ -f cache-$ID.tar ]; then
  OLD_MD5=`md5deep -q cache-$ID.tar`
  NEW_MD5=`md5deep -q cache-new.tar`
  if [ $OLD_MD5 == $NEW_MD5 ]; then
    echo "Same, not updating"
    rm cache-new.tar
  else
    echo "Changes detected... OLD: $OLD_MD5 , NEW: $NEW_MD5"
    mv cache-new.tar cache-$ID.tar
  fi
else
  mv cache-new.tar cache-$ID.tar
fi

kill -9 $PROGRESS_REPORTER_PID

