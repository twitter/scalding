package scalding

import sbt._
import Keys._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys._
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform._

import scala.collection.JavaConverters._

object ScaldingBuild extends Build {

  def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
    case version if version startsWith "2.10" => "2.10"
    case version if version startsWith "2.11" => "2.11"
    case _ => sys.error("unknown error")
  }
  def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"

  val scalaTestVersion = "2.2.2"
  val scalaCheckVersion = "1.11.5"
  val hadoopVersion = "2.4.0.t02"

  val algebirdVersion = "0.7.1"
  val bijectionVersion = "0.6.3"
  val chillVersion = "0.4.0"


  val slf4jVersion = "1.6.6"
  val elephantbirdVersion = "4.6"
  val hadoopLzoVersion = "0.4.16"
  val scaldingVersion = "0.13.2-t1430436062000-7caebcb0d1f96649981f6fda04cca3b371432e01"
  val json4sVersion = "3.2.6"

  lazy val cascadingJDBCVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "2.5.4")


  val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

  val sharedSettings = Project.defaultSettings ++ scalariformSettings ++ Seq(
    organization := "com.twitter",

    scalaVersion := "2.10.4",

    crossScalaVersions := Seq("2.10.4", "2.11.4"),

    ScalariformKeys.preferences := formattingPreferences,

    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

    javacOptions in doc := Seq("-source", "1.6"),

    libraryDependencies ++= Seq(
      "org.mockito" % "mockito-all" % "1.8.5" % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
    ),

    resolvers ++= Seq(
      "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
      "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com",
      "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Cloudera" at "https://artifactory.twitter.biz/java-virtual"
    ),

    printDependencyClasspath := {
      val cp = (dependencyClasspath in Compile).value
      cp.foreach(f => println(s"${f.metadata.get(moduleID.key)} => ${f.data}"))
    },

    fork in Test := true,

    javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m"),

    concurrentRestrictions in Global := Seq(
      Tags.limitAll(1)
    ),

    parallelExecution in Test := false,

    scalacOptions ++= Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-language:higherKinds", "-language:existentials"),

    scalacOptions <++= (scalaVersion) map { sv =>
        if (isScala210x(sv))
          Seq("-Xdivergence211")
        else
          Seq()
    },

    // Enables full stack traces in scalatest
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),

    // Publishing options:

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := {
      x => false
    },

    publishTo <<= version { v =>
      Some(
        "twttr" at "https://artifactory.twitter.biz/libs-releases-local"
      )
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
        <developers>
          <developer>
            <id>ianoc</id>
            <name>Ian O Connell</name>
          </developer>
        </developers>)
  ) ++ mimaDefaultSettings

  lazy val scalding = Project(
    id = "scalding-db",
    base = file("."),
    settings = sharedSettings
  ).settings(
    test := {},
    publish := {}, // skip publishing for this root project.
    publishLocal := {}
  ).aggregate(
    scaldingInternalDbCore,
    scaldingDBMacros
  )

  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference(AlignParameters, false).
      setPreference(PreserveSpaceBeforeArguments, true)
  }

  /**
   * This returns the youngest jar we released that is compatible with
   * the current.
   */
  val unreleasedModules = Set[String]() //releases 0.11

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map {
      s => "com.twitter" % ("scalding-db-" + s + "_2.10") % "0.13.0"
    }

  def module(name: String) = {
    val id = "scalding-db-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    )
  }

  lazy val cascadingVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.5.5")

  lazy val scaldingInternalDbCore = module("core").settings(
    libraryDependencies ++= Seq(
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-local" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-hadoop" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "scalding-core" % scaldingVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.json4s" %% "json4s-native" % json4sVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    )
  )

lazy val scaldingDBMacros = module("macros").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "com.twitter" %% "scalding-macros" % scaldingVersion,
      "org.scala-lang" % "scala-library" % scalaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion
    ) ++ (if(isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % "2.0.1") else Seq())
  },
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
  ).dependsOn(scaldingInternalDbCore)

}
