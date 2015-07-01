echo "Preparing environment... this make take ~10mins or more"
bash -c "while true; do echo -n .; sleep 5; done" &
PROGRESS_REPORTER_PID=$!

DEP_CACHE_PATH=$HOME/.scalding.dep.cache
ORIG_PATH=`pwd`
if [ -f $DEP_CACHE_PATH/cache.tar ]; then
  echo "Found cache, unpacking it"
  mkdir -p ~/.ivy2
  cd ~/.ivy2
  tar xf $DEP_CACHE_PATH/cache.tar
  echo "Unpacked deps"
else
  echo "No deps found, giving up. -- Would be good to have a cdn location to cache a bootstarp"
  # cd $DEP_CACHE_PATH
  # URL=$(curl https://www.dropbox.com/s/ggmfx38vmbzz0q8/cache.tar?dl=0 2> /dev/null | grep 'https://dl.dropboxusercontent.com' | sed -e's/.*href="\([^"]*\)".*/\1/g')
  # time curl -o cache.tar $URL && \
  # mkdir -p ~/.ivy2  && \
  # cd ~/.ivy2 && \
  # tar xf $DEP_CACHE_PATH/cache.tar
fi

if [ -f ~/.ivy2/cache/scalding-cache.tar.gz ]; then
  cd $ORIG_PATH
  tar zxf ~/.ivy2/cache/scalding-cache.tar.gz
  rm ~/.ivy2/cache/scalding-cache.tar.gz
fi

kill -9 $PROGRESS_REPORTER_PID

