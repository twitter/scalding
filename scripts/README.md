Testing info:

.travis.yml::

On travis to get better build time on weaker machines we split out the build into several targets.

.travis.yml contains several build targets splitting them up.


run_test.sh:
Here we unpack a cache(falling back to a static link if we can to try get some bootstrap jars). Run the compile step with standard out/standard error directed to /dev/null. (This is to stop travis giving up about our test logging being too verbose.). Then we run our test suite on our pre-build classes. Finally if anything has changed in our cache folders we pack them away into a special folder for travis to cache.

testValidator.sh:

Here we attempt in a bash script to ensure we have full coverage of our Build targets in our travis yml. Erroring the build if not the case.



NB: At some point on more powerful machines with a proper maven cache none of these strategies may be worth while. Possibly splitting up some of the test running, but the code around caches and such can all probably be deleted.