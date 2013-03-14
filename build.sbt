import AssemblyKeys._

name := "scalding"

version := "0.8.3-SNAPSHOT"

organization := "com.twitter"

//TODO: Change to 2.10.* when Twitter moves to Scala 2.10 internally
scalaVersion := "2.9.2"

crossScalaVersions := Seq("2.9.2", "2.10.0")

scalacOptions ++= Seq("-unchecked", "-deprecation")

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

// Use ScalaCheck
resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

//resolvers += "Twitter Artifactory" at "http://artifactory.local.twitter.com/libs-releases-local"

libraryDependencies += "cascading" % "cascading-core" % "2.0.7"

libraryDependencies += "cascading" % "cascading-local" % "2.0.7"

libraryDependencies += "cascading" % "cascading-hadoop" % "2.0.7"

libraryDependencies += "cascading.kryo" % "cascading.kryo" % "0.4.6"

libraryDependencies += "com.twitter" % "maple" % "0.2.5"

libraryDependencies += "com.twitter" %% "chill" % "0.2.0"

libraryDependencies += "com.twitter" %% "algebird-core" % "0.1.11"

libraryDependencies += "commons-lang" % "commons-lang" % "2.4"

libraryDependencies += "com.joestelmach" % "natty" % "0.7"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.1.3"

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
)

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
