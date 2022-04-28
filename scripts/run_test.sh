#!/bin/bash -exv

# Identify the bin dir in the distribution, and source the common include script
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR

withCmd() {
  CMD=$1
  for t in $TEST_TARGET; do echo -n "; $t/$CMD "; done
}

# collecting code coverage has a fairly high memory pressure
export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=128m -XX:+TieredCompilation -XX:MaxPermSize=256m -Xms512m -Xmx2g -Xss8m"

echo "Running test compilation..."
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true ++$TRAVIS_SCALA_VERSION "$(withCmd "test:compile")"

echo "Running test suite"
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true ++$TRAVIS_SCALA_VERSION "$(withCmd test)" jacocoReport
TST_EXIT_CODE=$?

echo "Running mima checks ... "
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true  ++$TRAVIS_SCALA_VERSION "$(withCmd mimaReportBinaryIssues)"

echo "Running compile:doc ... "
time ./sbt -Dhttp.keepAlive=false -Dsbt.repository.secure=true  ++$TRAVIS_SCALA_VERSION "$(withCmd compile:doc)"

echo "all test checks done"

# https://docs.codecov.com/docs/codecov-uploader
echo "uploading coverage report"

# some history about codecov:
# we originally tried to use the github action for codecov
# however, the uploader failed frequently due to upload rate limit issues
# we decided to use the binary directly to have fine control over backoff and retries

# verifying integrity of codecov uploader
curl https://keybase.io/codecovsecurity/pgp_keys.asc | gpg --no-default-keyring --keyring trustedkeys.gpg --import
curl -LO https://uploader.codecov.io/latest/linux/codecov
curl -LO https://uploader.codecov.io/latest/linux/codecov.SHA256SUM
curl -LO https://uploader.codecov.io/latest/linux/codecov.SHA256SUM.sig
gpgv codecov.SHA256SUM.sig codecov.SHA256SUM
shasum -a 256 -c codecov.SHA256SUM

# upload with backoff in case of rate limits which seems to happen frequently
# waits up to a maximum of ~17 minutes before failing
chmod +x codecov
UPLOAD_ATTEMPTS=10
for i in `seq 1 $UPLOAD_ATTEMPTS`; do
  UPLOAD_COMMAND="./codecov -Z"
  let UPLOAD_TIMEOUT=2**$i
  $UPLOAD_COMMAND && break || true
  if [[ $i == $UPLOAD_ATTEMPTS ]]; then
    echo "coverage upload failed, max retry"
    exit 1
  fi
  echo sleeping $UPLOAD_TIMEOUT seconds; sleep $UPLOAD_TIMEOUT
done