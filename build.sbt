import AssemblyKeys._

name := "scalding"

version := "0.8.3-SNAPSHOT"

organization := "com.twitter"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-unchecked", "-deprecation")

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

// Use ScalaCheck
resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

//resolvers += "Twitter Artifactory" at "http://artifactory.local.twitter.com/libs-releases-local"

libraryDependencies ++= {
  val cascadingVersion = "2.1.2"
  Seq("cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-local" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion,
      "cascading.kryo" % "cascading.kryo" % "0.4.7",
      "log4j" % "log4j" % "1.2.16", // Required for cascading.kryo...
      "org.apache.hadoop" % "hadoop-core" % "0.20.2",
      "com.twitter" % "maple" % "0.2.5",
      "com.twitter" % "chill_2.9.2" % "0.1.2",
      "com.twitter" % "algebird_2.9.2" % "0.1.6",
      "commons-lang" % "commons-lang" % "2.4",
      "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test",
      "com.joestelmach" % "natty" % "0.7",
      "io.backchat.jerkson" % "jerkson_2.9.2" % "0.7.0",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" % "specs_2.9.0-1" % "1.6.8" % "test"
  )
}

parallelExecution in Test := false

seq(assemblySettings: _*)

// Uncomment if you don't want to run all the tests before building assembly
// test in assembly := {}

// Janino includes a broken signature, and is not needed:
excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
    "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
  cp filter { jar => excludes(jar.data.getName)}
}

// Some of these files have duplicates, let's ignore:
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith("project.clj") => MergeStrategy.concat
    case s if s.endsWith(".html") => MergeStrategy.last
    case x => old(x)
  }
}
