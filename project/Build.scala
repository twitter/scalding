package scalding

import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys._
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform._
import com.twitter.scrooge.ScroogeSBT

import scala.collection.JavaConverters._

object ScaldingBuild extends Build {

  def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
    case version if version startsWith "2.10" => "2.10"
    case version if version startsWith "2.11" => "2.11"
    case _ => sys.error("unknown error")
  }
  def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"

  val algebirdVersion = "0.11.0"
  val avroVersion = "1.7.4"
  val bijectionVersion = "0.8.1"
  val cascadingAvroVersion = "3.0-SNAPSHOT"    // https://github.com/ScaleUnlimited/cascading.avro/pull/44
  val chillVersion = "0.7.1"
  val elephantbirdVersion = "4.11-SNAPSHOT"    // https://github.com/twitter/elephant-bird/pull/454
  val hadoopLzoVersion = "0.4.19"
  val hadoopVersion = "2.7.1"
  val hbaseVersion = "0.94.10"
  val hravenVersion = "0.9.17.t05"
  val jacksonVersion = "2.4.2"
  val json4SVersion = "3.2.11"
  val paradiseVersion = "2.0.1"
  val parquetVersion = "1.8.2-SNAPSHOT"        // https://github.com/apache/parquet-mr/pull/284
  val protobufVersion = "2.4.1"
  val quasiquotesVersion = "2.0.1"
  val scalaCheckVersion = "1.12.2"
  val scalaTestVersion = "2.2.4"
  val scalameterVersion = "0.6"
  val scroogeVersion = "3.20.0"
  val slf4jVersion = "1.6.6"
  val thriftVersion = "0.5.0"
  val junitVersion = "4.10"
  
  /* NOTE: the temp.cchepelov.* groupIds are to let the scalding build machine access the patched upstream dependencies until they get merged. 
    This *must* be removed before proceeding. */

  val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

  val sharedSettings = Project.defaultSettings ++ assemblySettings ++ scalariformSettings ++ Seq(
    organization := "com.twitter",

    scalaVersion := "2.11.7",

    crossScalaVersions := Seq("2.10.6", "2.11.7"),

    ScalariformKeys.preferences := formattingPreferences,

    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

    javacOptions in doc := Seq("-source", "1.6"),

    libraryDependencies ++= Seq(
      "org.mockito" % "mockito-all" % "1.8.5" % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
      "com.novocode" % "junit-interface" % "0.10" % "test"
    ),

    resolvers ++= Seq(
      "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
      "maven central" at "https://repo.maven.apache.org/maven2",
      "releases" at "https://oss.sonatype.org/content/repositories/releases",
      "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com",
      "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",

      "Conjars Repository" at "http://conjars.org/repo"   /* TEMPORARY: to get at the depencencies' snapshots while preparing the PR1446 branch */
    ),

    printDependencyClasspath := {
      val cp = (dependencyClasspath in Compile).value
      cp.foreach(f => println(s"${f.metadata.get(moduleID.key)} => ${f.data}"))
    },

    fork in Test := true,

    updateOptions := updateOptions.value.withConsolidatedResolution(true),

    updateOptions := updateOptions.value.withCachedResolution(true),

    aggregate in update := false,

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
    test in assembly := {},
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
        case s if s.endsWith("pom.properties") => MergeStrategy.last
        case s if s.endsWith("pom.xml") => MergeStrategy.last
        case s if s.endsWith(".jnilib") => MergeStrategy.rename
        case s if s.endsWith("jansi.dll") => MergeStrategy.rename
        case s if s.endsWith("properties") => MergeStrategy.filterDistinctLines
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
    scaldingHadoop2MR1Test,
    scaldingHadoop2TezTest,
    scaldingDb,
    maple,
    executionTutorial,
    scaldingSerialization,
    scaldingThriftMacros
  )

  lazy val scaldingAssembly = Project(
    id = "scalding-assembly",
    base = file("assembly"),
    settings = sharedSettings
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
    maple,
    scaldingSerialization
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
  val unreleasedModules = Set[String]("hadoop-test", "hadoop2-mr1-test", "hadoop2-tez") //releases 0.11

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map {
      s => "com.twitter" % ("scalding-" + s + "_2.10") % "0.15.0"
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
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "3.1.0-wip-52")

  lazy val tezVersion =
    System.getenv.asScala.getOrElse("SCALDING_TEZ_VERSION", "0.8.2") // should match what cascading-hadoop2-tez is using

  lazy val cascadingJDBCVersion =
    System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "3.0.0-wip-127")

  lazy val scaldingBenchmarks = module("benchmarks").settings(
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % scalameterVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Test := false
  ).dependsOn(scaldingCore)

  lazy val scaldingCore = module("core").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion % "provided",
      "cascading" % "cascading-local" % cascadingVersion,
      "com.twitter" % "chill-hadoop" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "chill-bijection" % chillVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "algebird-test" % algebirdVersion % "test",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-macros" % bijectionVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.scala-lang" % "scala-library" % scalaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided") ++
      (if (isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % quasiquotesVersion) else Seq())
    }, addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  ).dependsOn(scaldingArgs, scaldingDate, scaldingSerialization, maple)

  lazy val scaldingCommons = module("commons").settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      // TODO: split into scalding-protobuf
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "temp.cchepelov.com.twitter.elephantbird" % "elephant-bird-cascading3" % elephantbirdVersion,
      "temp.cchepelov.com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion,
      "com.hadoop.gplcompression" % "hadoop-lzo" % hadoopLzoVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion % "test",
      // TODO: split this out into scalding-thrift
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.thrift" % "libthrift" % thriftVersion,
      // TODO: split this out into a scalding-scrooge
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
      "junit" % "junit" % junitVersion % "test"
    )
  ).dependsOn(scaldingArgs, scaldingDate, scaldingCore, scaldingHadoopTest % "test")

  lazy val scaldingAvro = module("avro").settings(
    libraryDependencies ++= Seq(
      "temp.cchepelov.cascading.avro" % "avro-scheme" % cascadingAvroVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    )
  ).dependsOn(scaldingCore)

  lazy val scaldingParquet = module("parquet").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
      "temp.cchepelov.org.apache.parquet" % "parquet-cascading3" % parquetVersion // FIXME: https://github.com/apache/parquet-mr/pull/284
        exclude("temp.cchepelov.com.twitter.elephantbird", "elephant-bird-pig")
        exclude("temp.cchepelov.com.twitter.elephantbird", "elephant-bird-core")
        exclude("temp.cchepelov.org.apache.parquet", "parquet-pig")
        exclude("org.apache.parquet", "parquet-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-core"),
      "org.apache.thrift" % "libthrift" % "0.7.0",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion % "test",
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "com.twitter" %% "bijection-macros" % bijectionVersion,
      "com.twitter" %% "chill-bijection" % chillVersion
    ) ++ (if(isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % quasiquotesVersion) else Seq())
  }, addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full))
    .dependsOn(scaldingCore, scaldingHadoopTest % "test")

  import ScroogeSBT.autoImport._

  lazy val scaldingParquetScroogeFixtures = module("parquet-scrooge-fixtures")
    .settings(ScroogeSBT.newSettings:_*)
    .settings(
      scroogeThriftSourceFolder in Test <<= baseDirectory {
      base => base / "src/test/resources"
      },
      sourceGenerators in Test <+= (
          streams,
          scroogeThriftSources in Test,
          scroogeIsDirty in Test,
          sourceManaged
      ).map { (out, sources, isDirty, outputDir) =>
        // for some reason, sbt sometimes calls us multiple times, often with no source files.
        if (isDirty && sources.nonEmpty) {
          out.log.info("Generating scrooge thrift for %s ...".format(sources.mkString(", ")))
          ScroogeSBT.compile(out.log, outputDir, sources.toSet, Set(), Map(), "java", Set())
        }
        (outputDir ** "*.java").get.toSeq
      },
      libraryDependencies ++= Seq(
        "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided",
        "org.apache.thrift" % "libthrift" % thriftVersion
    )
  )

  lazy val scaldingParquetScrooge = module("parquet-scrooge")
    .settings(
      libraryDependencies ++= Seq(
        // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
        "temp.cchepelov.org.apache.parquet" % "parquet-cascading3" % parquetVersion
          exclude("temp.cchepelov.com.twitter.elephantbird", "elephant-bird-pig")
          exclude("temp.cchepelov.com.twitter.elephantbird", "elephant-bird-core")
          exclude("temp.cchepelov.org.apache.parquet", "parquet-pig")
          exclude("org.apache.parquet", "parquet-pig")
          exclude("com.twitter.elephantbird", "elephant-bird-pig")
          exclude("com.twitter.elephantbird", "elephant-bird-core"),
         "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.apache.parquet" % "parquet-thrift" % "1.8.1" /* FIXME: parquetVersion */ % "test" classifier "tests"
          exclude("temp.cchepelov.com.twitter.elephantbird", "elephant-bird-pig")
          exclude("temp.cchepelov.com.twitter.elephantbird", "elephant-bird-core")
          exclude("org.apache.parquet", "parquet-pig")
          exclude("com.twitter.elephantbird", "elephant-bird-pig")
          exclude("com.twitter.elephantbird", "elephant-bird-core"),
         "com.twitter" %% "scrooge-serializer" % scroogeVersion,
        "temp.cchepelov.com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion,
        "temp.cchepelov.com.twitter.elephantbird" % "elephant-bird-pig" % elephantbirdVersion,
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
        "cascading" % "cascading-hadoop" % hadoopVersion % "provided",
        "cascading" % "cascading-hadoop" % hadoopVersion % "test",
        "com.novocode" % "junit-interface" % "0.11" % "test",
        "junit" % "junit" % junitVersion % "test"

      )
  ).dependsOn(scaldingCore, scaldingParquet % "compile->compile;test->test", scaldingParquetScroogeFixtures % "test->test")


  lazy val scaldingHRaven = module("hraven").settings(
    libraryDependencies ++= Seq(
      "com.twitter.hraven" % "hraven-core" % hravenVersion
        // These transitive dependencies cause sbt to give a ResolveException
        // because they're not available on Maven. We don't need them anyway.
        // See https://github.com/twitter/cassie/issues/13
        exclude("javax.jms", "jms")
        exclude("com.sun.jdmk", "jmxtools")
        exclude("com.sun.jmx", "jmxri")

        // These transitive dependencies of hRaven cause conflicts when
        // running scalding-hraven/*assembly and aren't needed
        // for the part of the hRaven API that we use anyway
        exclude("com.twitter.common", "application-module-log")
        exclude("com.twitter.common", "application-module-stats")
        exclude("com.twitter.common", "args")
        exclude("com.twitter.common", "application"),
      "org.apache.hbase" % "hbase" % hbaseVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
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
        "jline" % "jline" % scalaVersion.take(4),
        "org.scala-lang" % "scala-compiler" % scalaVersion,
        "org.scala-lang" % "scala-reflect" % scalaVersion,
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "unprovided",
        "cascading" % "cascading-hadoop" % cascadingVersion /* % "provided" */, /* need to have a fabric around, can't just leave it to "provided" */
        "cascading" % "cascading-hadoop" % cascadingVersion % "unprovided",  /* for 'sbt run' */
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

  // zero dependency serialization module
  lazy val scaldingSerialization = module("serialization").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion
    ) ++ (if(isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % "2.0.1") else Seq())
  },
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
  )

  lazy val scaldingJson = module("json").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.json4s" %% "json4s-native" % json4SVersion,
      "temp.cchepelov.com.twitter.elephantbird" % "elephant-bird-cascading3" % elephantbirdVersion % "provided"
      )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingJdbc = module("jdbc").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "cascading" % "cascading-jdbc-core" % cascadingJDBCVersion,
      "cascading" % "cascading-jdbc-mysql" % cascadingJDBCVersion
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingHadoopTest = module("hadoop-test").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion classifier "tests",
      "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion classifier "tests",
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion classifier "tests",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion classifier "tests",
      "cascading" % "cascading-hadoop" % cascadingVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion % "test",
      "com.twitter" %% "chill-algebird" % chillVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    )
    }
  ).dependsOn(scaldingCore, scaldingSerialization)

  lazy val scaldingHadoop2MR1Test = module("hadoop2-mr1-test").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "cascading" % "cascading-hadoop2-mr1" % cascadingVersion % "test"
    ) }
  ).dependsOn(scaldingCore, scaldingSerialization, scaldingHadoopTest)

  lazy val scaldingHadoop2TezTest = module("hadoop2-tez-test").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.tez" % "tez-api" % tezVersion % "test",
      "org.apache.tez" % "tez-dag" % tezVersion % "test",
      "org.apache.tez" % "tez-mapreduce" % tezVersion % "test",
      "org.apache.tez" % "tez-mapreduce" % tezVersion % "test" classifier "tests",
      "cascading" % "cascading-hadoop2-tez" % cascadingVersion % "test"
    ) }
  ).dependsOn(scaldingCore, scaldingSerialization, scaldingHadoopTest)


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
    // Disable cross publishing for this artifact
    publishArtifact <<= (scalaVersion) { scalaVersion =>
        if(scalaVersion.startsWith("2.10")) false else true
        },
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.hbase" % "hbase" % hbaseVersion % "provided",
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion % "provided"
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
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
    }
  ).dependsOn(scaldingCore)

  lazy val scaldingDb = module("db").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.scala-lang" % "scala-library" % scalaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "com.twitter" %% "bijection-macros" % bijectionVersion
    ) ++ (if(isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % "2.0.1") else Seq())
  },
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
  ).dependsOn(scaldingCore)

lazy val scaldingThriftMacrosFixtures = module("thrift-macros-fixtures")
    .settings(ScroogeSBT.newSettings:_*)
    .settings(
      scroogeThriftSourceFolder in Test <<= baseDirectory {
      base => base / "src/test/resources"
      },
      libraryDependencies ++= Seq(
        "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided",
        "org.apache.thrift" % "libthrift" % thriftVersion
      )
  )

lazy val scaldingThriftMacros = module("thrift-macros")
    .settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion => Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "com.twitter" %% "bijection-macros" % bijectionVersion,
      "com.twitter" % "chill-thrift" % chillVersion % "test",
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided",
      "org.apache.thrift" % "libthrift" % thriftVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion  % "test",
      "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion classifier "tests",
      "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion classifier "tests",
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion classifier "tests",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion classifier "tests",
      "cascading" % "cascading-hadoop" % cascadingVersion % "test",
      "cascading" % "cascading-hadoop" % cascadingVersion classifier "tests"
    ) ++ (if (isScala210x(scalaVersion)) Seq("org.scalamacros" %% "quasiquotes" % "2.0.1") else Seq())
    },
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
  ).dependsOn(
      scaldingCore,
      scaldingHadoopTest % "test",
      scaldingSerialization,
      scaldingThriftMacrosFixtures % "test->test")
}
