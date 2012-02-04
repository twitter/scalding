import AssemblyKeys._

name := "scalding"

version := "0.2.0"

organization := "com.twitter"

scalaVersion := "2.8.1"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "cascading" % "cascading-core" % "2.0.0-wip-165"

libraryDependencies += "cascading" % "cascading-local" % "2.0.0-wip-165"

libraryDependencies += "cascading" % "cascading-hadoop" % "2.0.0-wip-165"

libraryDependencies += "commons-lang" % "commons-lang" % "2.4"

libraryDependencies += "de.javakaffee" % "kryo-serializers" % "0.9"

libraryDependencies += "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5" % "test"

parallelExecution in Test := false

seq(assemblySettings: _*)
