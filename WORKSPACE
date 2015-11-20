# TODO: This is totally broken for anyone but me. Figure out the right
# way to point to these tools from scalding
local_repository(
  name = "bazel_tools",
  path = "/Users/oscar/.bazel/base_workspace/tools")

local_repository(
  name = "bazel_third_party",
  path = "/Users/oscar/.bazel/base_workspace/third_party")

maven_server(
  name = "concurrent-maven",
  url = "http://conjars.org/repo")

maven_jar(
  name = "cascading-core",
  artifact = "cascading:cascading-core:2.6.1",
  server = "concurrent-maven")

maven_jar(
  name = "cascading-hadoop",
  artifact = "cascading:cascading-hadoop:2.6.1",
  server = "concurrent-maven")

maven_jar(
  name = "cascading-local",
  artifact = "cascading:cascading-local:2.6.1",
  server = "concurrent-maven")

maven_jar(
  name = "chill-hadoop",
  artifact = "com.twitter:chill-hadoop:0.7.1")

maven_jar(
  name = "chill-java",
  artifact = "com.twitter:chill-java:0.7.1")

maven_jar(
  name = "chill-bijection",
  artifact = "com.twitter:chill-bijection_2.11:0.7.1")

maven_jar(
  name = "chill",
  artifact = "com.twitter:chill_2.11:0.7.1")

maven_jar(
  name = "chill-algebird",
  artifact = "com.twitter:chill-algebird_2.11:0.7.1")

maven_jar(name="commons-cli", artifact="commons-cli:commons-cli:1.3.1")
maven_jar(name="commons-codec", artifact="commons-codec:commons-codec:1.7")
maven_jar(name="commons-logging", artifact="commons-logging:commons-logging:1.2")

maven_jar(
  name = "algebird-core",
  artifact = "com.twitter:algebird-core_2.11:0.11.0")

maven_jar(
  name = "bijection-core",
  artifact = "com.twitter:bijection-core_2.11:0.8.1")

maven_jar(
  name = "bijection-macros",
  artifact = "com.twitter:bijection-macros_2.11:0.8.1")

hadoop_version = "2.5.0"
maven_jar(name="hadoop-client", artifact="org.apache.hadoop:hadoop-mapreduce-client-core:%s" % hadoop_version)
maven_jar(name="hadoop-common", artifact="org.apache.hadoop:hadoop-common:%s" % hadoop_version)
maven_jar(name="hadoop-annotations", artifact="org.apache.hadoop:hadoop-annotations:%s" % hadoop_version)

hbase_version = "0.94.10"
maven_jar(name="hbase", artifact="org.apache.hbase:hbase:%s" % hbase_version)
#maven_jar(name="hbase-server", artifact="org.apache.hbase:hbase-server:%s" % hbase_version)
#maven_jar(name="hbase-client", artifact="org.apache.hbase:hbase-client:%s" % hbase_version)

maven_jar(name="java-ewah", artifact="com.googlecode.javaewah:JavaEWAH:0.6.6")
maven_jar(name="jgraph", artifact="thirdparty:jgrapht-jdk1.6:0.8.1", server = "concurrent-maven")
maven_jar(name="kryo", artifact="com.esotericsoftware.kryo:kryo:2.21")

maven_jar(
  name = "riffle",
  artifact = "riffle:riffle:0.1-dev",
  server = "concurrent-maven")

maven_jar(
  name = "scala-library",
  artifact = "org.scala-lang:scala-library:2.11.7")

maven_jar(
  name = "scala-reflect",
  artifact = "org.scala-lang:scala-reflect:2.11.7")

maven_jar(name="slf4j-api", artifact="org.slf4j:slf4j-api:1.6.6")

maven_jar(name="slf4j-log4j12", artifact="org.slf4j:slf4j-log4j12:1.6.6")
