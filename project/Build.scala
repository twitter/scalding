package scalding

import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys._

import scala.collection.JavaConverters._

object ScaldingBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ assemblySettings ++ Seq(
    organization := "com.twitter",

    //TODO: Change to 2.10.* when Twitter moves to Scala 2.10 internally
    scalaVersion := "2.9.3",

    crossScalaVersions := Seq("2.9.3", "2.10.3"),

    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

    javacOptions in doc := Seq("-source", "1.6"),


    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
      "org.mockito" % "mockito-all" % "1.8.5" % "test"
    ),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "http://oss.sonatype.org/content/repositories/releases",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com"
    ),

    parallelExecution in Test := false,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    // Uncomment if you don't want to run all the tests before building assembly
    // test in assembly := {},

    // Publishing options:

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := {
      x => false
    },

    publishTo <<= version { v =>
      Some(
        if (v.trim.endsWith("SNAPSHOT"))
          Opts.resolver.sonatypeSnapshots
        else
          Opts.resolver.sonatypeStaging
          //"twttr" at "http://artifactory.local.twitter.com/libs-releases-local"
      )
    },

    // Janino includes a broken signature, and is not needed:
    excludedJars in assembly <<= (fullClasspath in assembly) map {
      cp =>
        val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
          "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
        cp filter {
          jar => excludes(jar.data.getName)
        }
    },
    // Some of these files have duplicates, let's ignore:
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case s if s.endsWith(".class") => MergeStrategy.last
        case s if s.endsWith("project.clj") => MergeStrategy.concat
        case s if s.endsWith(".html") => MergeStrategy.last
        case s if s.endsWith(".dtd") => MergeStrategy.last
        case s if s.endsWith(".xsd") => MergeStrategy.last
        case s if s.endsWith(".jnilib") => MergeStrategy.rename
        case s if s.endsWith("jansi.dll") => MergeStrategy.rename
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
    test := {},
    publish := {}, // skip publishing for this root project.
    publishLocal := {}
  ).aggregate(
    scaldingArgs,
    scaldingDate,
    scaldingCore,
    scaldingCommons,
    scaldingAvro,
    scaldingParquet,
    scaldingRepl,
    scaldingJson,
    scaldingJdbc,
    maple
  )

  /**
   * This returns the youngest jar we released that is compatible with
   * the current.
   */
  val unreleasedModules = Set[String]()

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map {
      s => "com.twitter" % ("scalding-" + s + "_2.9.2") % "0.8.5"
    }

  def module(name: String) = {
    val id = "scalding-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    )
  }

  lazy val scaldingArgs = module("args")

  lazy val scaldingDate = module("date")

  lazy val cascadingVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.5.2")

  lazy val cascadingJDBCVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "2.5.1")

  val hadoopVersion = "1.1.2"
  val algebirdVersion = "0.5.0"
  val bijectionVersion = "0.6.2"
  val chillVersion = "0.3.6"
  val slf4jVersion = "1.6.6"

  lazy val scaldingCore = module("core").settings(
    libraryDependencies ++= Seq(
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-local" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-hadoop" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    )
  ).dependsOn(scaldingArgs, scaldingDate, maple)

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
      // TODO: split into scalding-protobuf
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter.elephantbird" % "elephant-bird-cascading2" % "4.4",
      "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.16",
      // TODO: split this out into scalding-thrift
      "org.apache.thrift" % "libthrift" % "0.5.0",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
    )
  ).dependsOn(scaldingArgs, scaldingDate, scaldingCore)

  lazy val scaldingAvro = Project(
    id = "scalding-avro",
    base = file("scalding-avro"),
    settings = sharedSettings
  ).settings(
    name := "scalding-avro",
    previousArtifact := Some("com.twitter" % "scalding-avro_2.9.2" % "0.1.0"),
    libraryDependencies ++= Seq(
      "cascading.avro" % "avro-scheme" % "2.1.2",
      "org.apache.avro" % "avro" % "1.7.4",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
    )
  ).dependsOn(scaldingCore)

  lazy val scaldingParquet = Project(
    id = "scalding-parquet",
    base = file("scalding-parquet"),
    settings = sharedSettings
  ).settings(
    name := "scalding-parquet",
    //previousArtifact := Some("com.twitter" % "scalding-parquet_2.9.2" % "0.1.0"),
    previousArtifact := None,
    libraryDependencies ++= Seq(
      "com.twitter" % "parquet-cascading" % "1.4.0",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
    )
  ).dependsOn(scaldingCore)

  lazy val scaldingRepl = Project(
    id = "scalding-repl",
    base = file("scalding-repl"),
    settings = sharedSettings
  ).settings(
    name := "scalding-repl",
    previousArtifact := None,
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.scala-lang" % "jline" % scalaVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingJson = Project(
    id = "scalding-json",
    base = file("scalding-json"),
    settings = sharedSettings
  ).settings(
    name := "scalding-json",
    previousArtifact := None,
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.3"
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingJdbc = Project(
    id = "scalding-jdbc",
    base = file("scalding-jdbc"),
    settings = sharedSettings
  ).settings(
    name := "scalding-jdbc",
    previousArtifact := None,
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "cascading" % "cascading-jdbc-core" % cascadingJDBCVersion
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingHadoopTest = Project(
    id = "scalding-hadoop-test",
    base = file("scalding-hadoop-test"),
    settings = sharedSettings
  ).settings(
    name := "scalding-hadoop-test",
    previousArtifact := None,
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-test" % hadoopVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion
    )
    }
  ).dependsOn(scaldingCore)

  lazy val maple = Project(
    id = "maple",
    base = file("maple"),
    settings = sharedSettings
  ).settings(
    name := "maple",
    previousArtifact := None,
    crossPaths := false,
    autoScalaLibrary := false,
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.apache.hbase" % "hbase" % "0.94.5" % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
    }
  )
}
