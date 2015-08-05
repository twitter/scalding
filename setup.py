# Setup file for ReadTheDocs
#
# This script downloads a jdk, adds it to the path,
# runs sbt compile, and then uses sbt/tut to
# generate the documentation before Sphinx
# converts it to html.
import os
import sys

os.system("uname -a")
print("Setting path")
os.environ["PATH"] = os.getcwd() + "/openjdk-1.7.0-u80-unofficial-linux-amd64-image/bin:" + os.environ["PATH"]

# Even though the ReadTheDocs machine has Intel architecture, we download
# an amd64 version of the JDK because the Intel 32-bit version doesn't
# work on the remote machine. (The machine is a 64-bit machine and doesn't
# seem to have the compatibility libraries to run 32-bit executables.)
#
# Note: this script depends on alexkasko's unofficial JDK builds
# (https://github.com/alexkasko/openjdk-unofficial-builds). We hope that
# he continues to host these JDKs.
os.system("""
wget https://bitbucket.org/alexkasko/openjdk-unofficial-builds/downloads/openjdk-1.7.0-u80-unofficial-linux-amd64-image.zip &&
unzip -q openjdk-1.7.0-u80-unofficial-linux-amd64-image.zip &&
./sbt docs/tut
""")
print("Done setting up")
