package scalding

import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import sbtgitflow.ReleasePlugin._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys._

import scala.collection.JavaConverters._

object ScaldingBuild extends Build {
  def withCross(dep: ModuleID) =
    dep cross CrossVersion.binaryMapped {
      case "2.9.3" => "2.9.2"
      case version if version startsWith "2.10" => "2.10" // TODO: hack because sbt is broken
      case x => x
    }

  val sharedSettings = Project.defaultSettings ++ assemblySettings ++
  releaseSettings ++ Seq(
    organization := "com.twitter",

    //TODO: Change to 2.10.* when Twitter moves to Scala 2.10 internally
    scalaVersion := "2.9.3",

    crossScalaVersions := Seq("2.9.3", "2.10.0"),

    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
      "org.mockito" % "mockito-all" % "1.8.5" % "test"
    ),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com",
      "Twitter SVN Maven" at "https://svn.twitter.biz/maven-public"
    ),

    parallelExecution in Test := false,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    // Uncomment if you don't want to run all the tests before building assembly
    // test in assembly := {},

    // Publishing options:

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("sonatype-releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    // Janino includes a broken signature, and is not needed:
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
                         "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
      cp filter { jar => excludes(jar.data.getName)}
    },

    // Some of these files have duplicates, let's ignore:
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
      {
        case s if s.endsWith(".class") => MergeStrategy.last
        case s if s.endsWith("project.clj") => MergeStrategy.concat
        case s if s.endsWith(".html") => MergeStrategy.last
        case s if s.endsWith(".dtd") => MergeStrategy.last
        case s if s.endsWith(".xsd") => MergeStrategy.last
        case x => old(x)
      }
    },

    pomExtra := (
      <url>https://github.com/twitter/scalding</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:twitter/scalding.git</url>
        <connection>scm:git:git@github.com:twitter/scalding.git</connection>
      </scm>
      <developers>
        <developer>
          <id>posco</id>
          <name>Oscar Boykin</name>
          <url>http://twitter.com/posco</url>
        </developer>
        <developer>
          <id>avibryant</id>
          <name>Avi Bryant</name>
          <url>http://twitter.com/avibryant</url>
        </developer>
        <developer>
          <id>argyris</id>
          <name>Argyris Zymnis</name>
          <url>http://twitter.com/argyris</url>
        </developer>
      </developers>)
  ) ++ mimaDefaultSettings

  lazy val scalding = Project(
    id = "scalding",
    base = file("."),
    settings = sharedSettings ++ DocGen.publishSettings
  ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(
    scaldingArgs,
    scaldingDate,
    scaldingCore,
    scaldingCommons
  )

  /**
    * This returns the youngest jar we released that is compatible with
    * the current.
    */
  val unreleasedModules = Set[String]()
  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map { s => "com.twitter" % ("scalding-" + s + "_2.9.2") % "0.8.5" }

  def module(name: String) = {
    val id = "scalding-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    )
  }

  lazy val scaldingArgs = module("args")

  lazy val scaldingDate = module("date").settings(
    libraryDependencies += "com.joestelmach" % "natty" % "0.7"
  )

  lazy val cascadingVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.1.6")

  val algebirdVersion = "0.2.0"
  val bijectionVersion = "0.5.2"
  val chillVersion = "0.3.0"

  lazy val scaldingCore = module("core").settings(
    libraryDependencies ++= Seq(
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-local" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion,
      "com.twitter" % "maple" % "0.2.7",
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-hadoop" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "commons-lang" % "commons-lang" % "2.4",
      withCross("com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.1.3"),
      "org.apache.hadoop" % "hadoop-core" % "0.20.2" % "provided",
      "org.slf4j" % "slf4j-api" % "1.6.6",
      "org.slf4j" % "slf4j-log4j12" % "1.6.6" % "provided"
    )
  ).dependsOn(scaldingArgs, scaldingDate)

  lazy val scaldingCommons = Project(
    id = "scalding-commons",
    base = file("scalding-commons"),
    settings = sharedSettings
  ).settings(
    name := "scalding-commons",
    previousArtifact := Some("com.twitter" % "scalding-commons_2.9.2" % "0.2.0"),
    libraryDependencies ++= Seq(
      "com.backtype" % "dfs-datastores-cascading" % "1.3.4",
      "com.backtype" % "dfs-datastores" % "1.3.4",
      "commons-io" % "commons-io" % "2.4",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter.elephantbird" % "elephant-bird-cascading2" % "3.0.6",
      "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.16",
      "org.apache.thrift" % "libthrift" % "0.5.0",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-log4j12" % "1.6.6",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
    )
  ).dependsOn(scaldingArgs, scaldingDate, scaldingCore)

}
