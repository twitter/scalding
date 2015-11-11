def make_dep(name, org, version, artifact_name=None, tpe=jar):
  artifact_name = artifact_name or name
  return jar_library(
    name=name,
    jars=[
      tpe(org, artifact_name, version),
    ],
  )

make_dep(
  name = "cascading-core",
  org = "cascading", version = "2.6.1")

make_dep(
  name = "cascading-hadoop",
  org = "cascading", version = "2.6.1")

make_dep(
  name = "cascading-local",
  org = "cascading", version = "2.6.1")

make_dep(
  name = "chill-hadoop",
  org = "com.twitter", version = "0.7.1")

make_dep(
  name = "chill-java",
  org = "com.twitter", version = "0.7.1")

make_dep(
  name = "chill-bijection",
  org = "com.twitter", version = "0.7.1", tpe=scala_jar)

make_dep(
  name = "chill",
  org = "com.twitter", version = "0.7.1", tpe=scala_jar)

make_dep(
  name = "chill-algebird",
  org = "com.twitter", version = "0.7.1", tpe=scala_jar)

make_dep(name="commons-cli", org = "commons-cli", version = "1.3.1")
make_dep(name="commons-codec", org = "commons-codec", version = "1.7")
make_dep(name="commons-logging", org = "commons-logging", version = "1.2")

make_dep(
  name = "algebird-core",
  org = "com.twitter", version = "0.11.0", tpe=scala_jar)

make_dep(
  name = "bijection-core",
  org = "com.twitter", version = "0.8.1", tpe=scala_jar)

make_dep(
  name = "bijection-macros",
  org = "com.twitter", version = "0.8.1", tpe=scala_jar)

hadoop_version = "2.5.0"
make_dep(name="hadoop-client", org = "org.apache.hadoop", version = hadoop_version)
make_dep(name="hadoop-common", org = "org.apache.hadoop", version = hadoop_version)
make_dep(name="hadoop-annotations", org = "org.apache.hadoop", version = hadoop_version)

hbase_version = "0.94.10"
make_dep(name="hbase", org = "org.apache.hbase", version = hbase_version)
#make_dep(name="hbase-server", org = "org.apache.hbase", version = hbase_version)
#make_dep(name="hbase-client", org = "org.apache.hbase", version = hbase_version)

make_dep(name="java-ewah", org = "com.googlecode.javaewah", version = "0.6.6", artifact_name = "JavaEWAH")
make_dep(name="jgraph", org = "thirdparty", version = "0.8.1", artifact_name = "jgrapht-jdk1.6")
make_dep(name="kryo", org = "com.esotericsoftware.kryo", version = "2.21")

make_dep(
  name = "riffle",
  org = "riffle", version = "0.1-dev")

make_dep(
  name = "scala-library",
  org = "org.scala-lang", version = "2.11.7")

make_dep(
  name = "scala-reflect",
  org = "org.scala-lang", version = "2.11.7")

make_dep(name="slf4j-api", org = "org.slf4j", version = "1.6.6")

make_dep(name="slf4j-log4j12", org = "org.slf4j", version = "1.6.6")

# Test libraries.

make_dep(
  name = "scalacheck",
  org = "org.scalacheck", version = "1.12.2", tpe=scala_jar)

make_dep(
  name = "scalatest",
  org = "org.scalatest", version = "2.2.4", tpe=scala_jar)

make_dep(
  name = "junit",
  org = "junit", version = "4.10")
