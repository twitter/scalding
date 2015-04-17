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
    case version if version startsWith "2.10" => "2.10"
    case version if version startsWith "2.11" => "2.11"
    case _ => sys.error("unknown error")
  }
  def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"

  val algebirdVersion = "0.9.0"
  val avroVersion = "1.7.4"
  val bijectionVersion = "0.7.2"
  val cascadingAvroVersion = "2.1.2"
  val chillVersion = "0.5.2"
  val dfsDatastoresVersion = "1.3.4"
  val elephantbirdVersion = "4.6"
  val hadoopLzoVersion = "0.4.16"
  val hadoopVersion = "1.2.1"
  val hbaseVersion = "0.94.10"
  val hravenVersion = "0.9.13"
  val jacksonVersion = "2.4.2"
  val json4SVersion = "3.2.11"
  val parquetVersion = "1.6.0rc4"
  val protobufVersion = "2.4.1"
  val scalaCheckVersion = "1.12.2"
  val scalaTestVersion = "2.2.4"
  val scalameterVersion = "0.6"
  val scroogeVersion = "3.17.0"
  val slf4jVersion = "1.6.6"
  val thriftVersion = "0.7.0"

  val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

  val sharedSettings = Project.defaultSettings ++ assemblySettings ++ scalariformSettings ++ Seq(
    organization := "com.twitter",

    scalaVersion := "2.10.4",

    crossScalaVersions := Seq("2.10.4", "2.11.5"),

    ScalariformKeys.preferences := formattingPreferences,

    updateOptions := updateOptions.value.withCachedResolution(true),

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
      "maven central" at "http://repo.maven.apache.org/maven2",
      "releases" at "https://oss.sonatype.org/content/repositories/releases",
      "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Twitter Maven" at "http://maven.twttr.com" // Needed for hadoop-lzo, last on list as its flaky seemingly.
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
    scaldingMacros,
    maple,
    executionTutorial
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
      s => "com.twitter" % ("scalding-" + s + "_2.10") % "0.13.0"
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
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.6.1")

  lazy val cascadingJDBCVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "2.6.0")

  lazy val scaldingBenchmarks = module("benchmarks").settings(
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % scalameterVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Test := false
  ).dependsOn(scaldingCore, scaldingMacros)

  lazy val scaldingCore = module("core").settings(
    libraryDependencies ++= Seq(
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-local" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-hadoop" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "algebird-test" % algebirdVersion % "test",
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    )
  ).dependsOn(scaldingArgs, scaldingDate, maple)

  lazy val scaldingCommons = module("commons").settings(
    libraryDependencies ++= Seq(
      "com.backtype" % "dfs-datastores-cascading" % dfsDatastoresVersion,
      "com.backtype" % "dfs-datastores" % dfsDatastoresVersion,
      // TODO: split into scalding-protobuf
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter.elephantbird" % "elephant-bird-cascading2" % elephantbirdVersion,
      "com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion,
      "com.hadoop.gplcompression" % "hadoop-lzo" % hadoopLzoVersion,
      // TODO: split this out into scalding-thrift
      "org.apache.thrift" % "libthrift" % thriftVersion % "provided",
      // TODO: split this out into a scalding-scrooge
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    )
  ).dependsOn(scaldingArgs, scaldingDate, scaldingCore, scaldingHadoopTest % "test")

  lazy val scaldingAvro = module("avro").settings(
    libraryDependencies ++= Seq(
      "cascading.avro" % "avro-scheme" % cascadingAvroVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
    )
  ).dependsOn(scaldingCore)

  lazy val scaldingParquet = module("parquet").settings(
    libraryDependencies ++= Seq(
      // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
      "com.twitter" % "parquet-cascading" % parquetVersion
        exclude("com.twitter", "parquet-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-core"),
      "org.apache.thrift" % "libthrift" % "0.7.0",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
    )
  ).dependsOn(scaldingCore)

  def scaldingParquetScroogeDeps(version: String) = {
    if (isScala210x(version))
      Seq(
        // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
        "com.twitter" % "parquet-cascading" % parquetVersion
          exclude("com.twitter", "parquet-pig")
          exclude("com.twitter.elephantbird", "elephant-bird-pig")
          exclude("com.twitter.elephantbird", "elephant-bird-core"),
        "com.twitter" %% "parquet-scrooge" % parquetVersion,
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
      )
    else
      Seq()
  }

  lazy val scaldingParquetScrooge = module("parquet-scrooge").settings(
    skip in compile := !(isScala210x(scalaVersion.value)),
    skip in test := !(isScala210x(scalaVersion.value)),
    publishArtifact := isScala210x(scalaVersion.value),
    libraryDependencies ++= scaldingParquetScroogeDeps(scalaVersion.value)
  ).dependsOn(scaldingCore, scaldingParquet % "compile->compile;test->test")

  lazy val scaldingHRaven = module("hraven").settings(
    libraryDependencies ++= Seq(
      "com.twitter.hraven" % "hraven-core" % hravenVersion,
      "org.apache.hbase" % "hbase" % hbaseVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
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
      skip in compile := !isScala210x(scalaVersion.value),
      skip in test := !isScala210x(scalaVersion.value),
      publishArtifact := isScala210x(scalaVersion.value),
      initialCommands in console := """
        import com.twitter.scalding._
        import com.twitter.scalding.ReplImplicits._
        import com.twitter.scalding.ReplImplicitContext._
        """,
      libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
        "jline" % "jline" % scalaVersion.take(4),
        "org.scala-lang" % "scala-compiler" % scalaVersion,
        "org.scala-lang" % "scala-reflect" % scalaVersion,
        "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided",
        "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "unprovided",
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "unprovided"
      )
      },
      // https://gist.github.com/djspiewak/976cd8ac65e20e136f05
      unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / s"scala-${scalaBinaryVersion(scalaVersion.value)}"
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
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.json4s" %% "json4s-native" % json4SVersion,
      "com.twitter.elephantbird" % "elephant-bird-cascading2" % elephantbirdVersion % "provided"
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
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingMacros = module("macros").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.scala-lang" % "scala-library" % scalaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "com.twitter" %% "bijection-macros" % bijectionVersion
    ) ++ (if(isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % "2.0.1") else Seq())
  },
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
  ).dependsOn(scaldingCore, scaldingHadoopTest)

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
      "org.apache.hbase" % "hbase" % hbaseVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
    }
  )

  lazy val executionTutorial = Project(
    id = "execution-tutorial",
    base = file("tutorial/execution-tutorial"),
    settings = sharedSettings
  ).settings(
    name := "execution-tutorial",
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.scala-lang" % "scala-library" % scalaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
    }
  ).dependsOn(scaldingCore)
}
