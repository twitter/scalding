load("/bazel_tools/scala", "scala_library")

scala_library(
  name = "scalding-serialization",
  srcs = glob(["**/*.scala"]),
  deps = [":scalding-serialization-java"],
  visibility = ["//visibility:public"]
  )

java_library(
  name = "scalding-serialization-java",
  srcs = glob(["**/*.java"]),
  visibility = ["//visibility:public"]
  )
