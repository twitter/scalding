load("/bazel_tools/scala", "scala_library")

java_library(
  name ="core-java",
  srcs = glob(["**/*.java"]),
  deps = [
    "@scala-library//jar",
    "@cascading-core//jar",
    ])

scala_library(
  name = "scalding-core",
  srcs = glob(["**/*.scala"]),
  deps = [
    "//maple/src/main:maple",
    "//scalding-args/src/main/scala/com/twitter/scalding:scalding-args",
    "//scalding-date/src/main/scala/com/twitter/scalding:scalding-date",
    "//scalding-serialization/src/main:scalding-serialization",
    ":core-java",
    "@algebird-core//jar",
    "@bijection-core//jar",
    "@bijection-macros//jar",
    "@cascading-core//jar",
    "@cascading-hadoop//jar",
    "@cascading-local//jar",
    "@chill-algebird//jar",
    "@chill-bijection//jar",
    "@chill-hadoop//jar",
    "@chill-java//jar",
    "@chill//jar",
    "@commons-cli//jar",
    "@hadoop-annotations//jar",
    "@hadoop-client//jar",
    "@hadoop-common//jar",
    "@java-ewah//jar",
    "@jgraph//jar",
    "@kryo//jar",
    "@riffle//jar",
    "@scala-library//jar",
    "@scala-reflect//jar",
    "@slf4j-api//jar",
    ]
  )
