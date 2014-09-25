package scalding

import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys._
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform._

import scala.collection.JavaConverters._

object ScaldingBuild extends Build {

  def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {    
    case version if version startsWith "2.9" => "2.9"
    case version if version startsWith "2.10" => "2.10"
  }

  val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

  val sharedSettings = Project.defaultSettings ++ assemblySettings ++ scalariformSettings ++ Seq(
    organization := "com.twitter",

    //TODO: Change to 2.10.* when Twitter moves to Scala 2.10 internally
    scalaVersion := "2.9.3",

    crossScalaVersions := Seq("2.9.3", "2.10.4"),

    ScalariformKeys.preferences := formattingPreferences,

    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

    javacOptions in doc := Seq("-source", "1.6"),

    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
      "org.mockito" % "mockito-all" % "1.8.5" % "test"
    ),

    resolvers ++= Seq(
      "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
      "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com",
      "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
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

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    // Uncomment if you don't want to run all the tests before building assembly
    // test in assembly := {},
    logLevel in assembly := Level.Warn,

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
    scaldingParquetScrooge,
    scaldingHRaven,
    scaldingRepl,
    scaldingJson,
    scaldingJdbc,
    scaldingHadoopTest,
    maple
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
  val unreleasedModules = Set[String]("hadoop-test") //releases 0.11

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map {
      s => "com.twitter" % ("scalding-" + s + "_2.9.3") % "0.11.0"
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
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.5.5")

  lazy val cascadingJDBCVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "2.5.4")

  val hadoopVersion = "1.2.1"
  val algebirdVersion = "0.7.1"
  val bijectionVersion = "0.6.3"
  val chillVersion = "0.4.0"
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

  lazy val scaldingCommons = module("commons").settings(
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

  lazy val scaldingAvro = module("avro").settings(
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

  lazy val scaldingParquet = module("parquet").settings(
    libraryDependencies ++= Seq(
      "com.twitter" % "parquet-cascading" % "1.6.0rc2",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
    )
  ).dependsOn(scaldingCore)
  
  def scaldingParquetScroogeDeps(version: String) = {
    if (scalaBinaryVersion(version) == "2.9") 
      Seq() 
    else
      Seq(
        "com.twitter" % "parquet-cascading" % "1.6.0rc2",
        "com.twitter" %% "parquet-scrooge" % "1.6.0rc2",
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
        "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
      )
  }
  
  lazy val scaldingParquetScrooge = module("parquet-scrooge").settings(
    skip in compile := scalaBinaryVersion(scalaVersion.value) == "2.9",
    skip in test := scalaBinaryVersion(scalaVersion.value) == "2.9",
    publishArtifact := !(scalaBinaryVersion(scalaVersion.value) == "2.9"),
    libraryDependencies ++= scaldingParquetScroogeDeps(scalaVersion.value)
  ).dependsOn(scaldingCore, scaldingParquet % "compile->compile;test->test")

  lazy val scaldingHRaven = module("hraven").settings(
    libraryDependencies ++= Seq(
      "com.twitter.hraven" % "hraven-core" % "0.9.13",
      "org.apache.hbase" % "hbase" % "0.94.10",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
    )
  ).dependsOn(scaldingCore)

  // create new configuration which will hold libs otherwise marked as 'provided'
  // so that we can re-include them in 'run'. unfortunately, we still have to
  // explicitly add them to both 'provided' and 'unprovided', as below
  // solution borrowed from: http://stackoverflow.com/a/18839656/1404395
  val Unprovided = config("unprovided") extend Runtime

  lazy val scaldingRepl = module("repl")
    .configs(Unprovided) // include 'unprovided' as config option
    .settings(
      initialCommands in console := """
        import com.twitter.scalding._
        import com.twitter.scalding.ReplImplicits._
        import com.twitter.scalding.ReplImplicitContext._
        """,
      libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
        "org.scala-lang" % "jline" % scalaVersion,
        "org.scala-lang" % "scala-compiler" % scalaVersion,
        "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
        "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "unprovided",
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "unprovided"
      )
      }
  ).dependsOn(scaldingCore)
  // run with 'unprovided' config includes libs marked 'unprovided' in classpath
  .settings(inConfig(Unprovided)(Classpaths.configSettings ++ Seq(
    run <<= Defaults.runTask(fullClasspath, mainClass in (Runtime, run), runner in (Runtime, run))
  )): _*)
  .settings(
    // make scalding-repl/run use 'unprovided' config
    run <<= (run in Unprovided)
  )

  lazy val scaldingJson = module("json").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.3"
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingJdbc = module("jdbc").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "cascading" % "cascading-jdbc-core" % cascadingJDBCVersion,
      "cascading" % "cascading-jdbc-mysql" % cascadingJDBCVersion
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingHadoopTest = module("hadoop-test").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      ("org.apache.hadoop" % "hadoop-core" % hadoopVersion),
      ("org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion),
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion
    )
    }
  ).dependsOn(scaldingCore)

  // This one uses a different naming convention
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
