echo "Packing environment after success... this make take ~10mins or more"
bash -c "while true; do echo -n .; sleep 5; done" &

ORIG_PATH=`pwd`

PROGRESS_REPORTER_PID=$!
DEP_CACHE_PATH=$HOME/.scalding.dep.cache
mkdir -p $DEP_CACHE_PATH

cd $ORIG_PATH
tar zcf ~/.ivy2/cache/scalding-cache.tar.gz project/target

cd ~/.ivy2

tar cf $DEP_CACHE_PATH/cache-new.tar cache

cd $DEP_CACHE_PATH

if [ -f cache.tar ]; then
  OLD_MD5=`md5deep -q cache.tar`
  NEW_MD5=`md5deep -q cache-new.tar`
  if [ $OLD_MD5 == $NEW_MD5 ]; then
    echo "Same, not updating"
    rm cache-new.tar
  else
    echo "Changes detected... OLD: $OLD_MD5 , NEW: $NEW_MD5"
    mv cache-new.tar cache.tar
  fi
else
  mv cache-new.tar cache.tar
fi

kill -9 $PROGRESS_REPORTER_PID

