import AssemblyKeys._
import ReleaseTransformations._
import com.typesafe.sbt.SbtGhPages.GhPagesKeys._
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import sbtassembly.Plugin._
import sbtunidoc.Plugin.UnidocKeys._
import scala.collection.JavaConverters._
import scalariform.formatter.preferences._

def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
  case version if version startsWith "2.10" => "2.10"
  case version if version startsWith "2.11" => "2.11"
  case version if version startsWith "2.12" => "2.12"
  case _ => sys.error("unknown error")
}

val algebirdVersion = "0.13.0"
val apacheCommonsVersion = "2.2"
val avroVersion = "1.7.4"
val bijectionVersion = "0.9.5"
val cascadingAvroVersion = "2.1.2"
val chillVersion = "0.8.4"
val elephantbirdVersion = "4.15"
val hadoopLzoVersion = "0.4.19"
val hadoopVersion = "2.5.0"
val hbaseVersion = "0.94.10"
val hravenVersion = "0.9.17.t05"
val jacksonVersion = "2.8.7"
val json4SVersion = "3.5.0"
val paradiseVersion = "2.1.0"
val parquetVersion = "1.8.1"
val protobufVersion = "2.4.1"
val scalameterVersion = "0.8.2"
val scalaCheckVersion = "1.13.4"
val scalaTestVersion = "3.0.1"
val scroogeVersion = "4.12.0"
val slf4jVersion = "1.6.6"
val thriftVersion = "0.5.0"
val junitVersion = "4.10"
val macroCompatVersion = "1.1.1"
val jlineVersion = "2.14.3"

val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

val sharedSettings = assemblySettings ++ scalariformSettings ++ Seq(
  organization := "com.twitter",

  scalaVersion := "2.11.8",

  crossScalaVersions := Seq(scalaVersion.value, "2.12.1"),

  ScalariformKeys.preferences := formattingPreferences,

  javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

  javacOptions in doc := Seq("-source", "1.6"),

  wartremoverErrors in (Compile, compile) += Wart.OptionPartial,

  libraryDependencies ++= Seq(
    "org.mockito" % "mockito-all" % "1.8.5" % "test",
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
    "com.novocode" % "junit-interface" % "0.10" % "test"
  ),

  resolvers ++= Seq(
    Opts.resolver.mavenLocalFile,
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases,
    "Concurrent Maven Repo" at "http://conjars.org/repo",
    "Twitter Maven" at "http://maven.twttr.com",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  ),

  printDependencyClasspath := {
    val cp = (dependencyClasspath in Compile).value
    cp.foreach(f => println(s"${f.metadata.get(moduleID.key)} => ${f.data}"))
  },

  fork in Test := true,

  updateOptions := updateOptions.value.withCachedResolution(true),

  aggregate in update := false,

  javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m"),

  concurrentRestrictions in Global := Seq(
    Tags.limitAll(1)
  ),

  parallelExecution in Test := false,

  scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials"
    ),

  scalacOptions in(Compile, doc) ++= Seq(scalaVersion.value).flatMap {
    case v if v.startsWith("2.12") => Seq("-no-java-comments") //workaround for scala/scala-dev#249
    case _ => Seq()
  },

  /**
   * add linter for common scala issues:
   * https://github.com/HairyFotr/linter
   */
  addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.17"),

  // Enables full stack traces in scalatest
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),

  // Uncomment if you don't want to run all the tests before building assembly
  // test in assembly := {},
  logLevel in assembly := Level.Warn,

  // Publishing options:
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseVersionBump := sbtrelease.Version.Bump.Minor, // need to tweak based on mima results
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { x => false },

  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
    pushChanges),

  publishTo := Some(
      if (version.value.trim.endsWith("SNAPSHOT"))
        Opts.resolver.sonatypeSnapshots
      else Opts.resolver.sonatypeStaging
    ),

  // Janino includes a broken signature, and is not needed:
  excludedJars in assembly := {
    val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
      (fullClasspath in assembly).value filter {
        jar => excludes(jar.data.getName)
      }
  },
  // Some of these files have duplicates, let's ignore:
  mergeStrategy in assembly :=  {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith("project.clj") => MergeStrategy.concat
    case s if s.endsWith(".html") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.last
    case s if s.endsWith(".xsd") => MergeStrategy.last
    case s if s.endsWith("pom.properties") => MergeStrategy.last
    case s if s.endsWith("pom.xml") => MergeStrategy.last
    case s if s.endsWith(".jnilib") => MergeStrategy.rename
    case s if s.endsWith("jansi.dll") => MergeStrategy.rename
    case s if s.endsWith("libjansi.so") => MergeStrategy.rename
    case s if s.endsWith("properties") => MergeStrategy.filterDistinctLines
    case x => (mergeStrategy in assembly).value(x)
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
  settings = sharedSettings)
 .settings(noPublishSettings)
 .aggregate(
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
  scaldingDb,
  maple,
  executionTutorial,
  scaldingSerialization,
  scaldingThriftMacros
)

lazy val scaldingAssembly = Project(
  id = "scalding-assembly",
  base = file("assembly"),
  settings = sharedSettings)
 .settings(noPublishSettings)
 .aggregate(
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

lazy val noPublishSettings = Seq(
    publish := (),
    publishLocal := (),
    test := (),
    publishArtifact := false
  )

/**
 * This returns the youngest jar we released that is compatible with
 * the current.
 */
val ignoredModules = Set[String]("benchmarks")

def youngestForwardCompatible(subProj: String) =
  Some(subProj)
    .filterNot(ignoredModules.contains(_))
    .map {
    s => "com.twitter" %% (s"scalding-$s") % "0.17.0"
  }

def module(name: String) = {
  val id = "scalding-%s".format(name)
  Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
    Keys.name := id,
    mimaPreviousArtifacts := youngestForwardCompatible(name).toSet)
  )
}

lazy val scaldingArgs = module("args")

lazy val scaldingDate = module("date")

lazy val cascadingVersion =
  System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.6.1")

lazy val cascadingJDBCVersion =
  System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "2.6.0")

lazy val scaldingBenchmarks = module("benchmarks")
  .settings(
    libraryDependencies ++= Seq(
        "com.storm-enroute" %% "scalameter" % scalameterVersion % "test",
        "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
      ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Test := false
  ).dependsOn(scaldingCore)

lazy val scaldingCore = module("core").settings(
  libraryDependencies ++= Seq(
    "cascading" % "cascading-core" % cascadingVersion,
    "cascading" % "cascading-hadoop" % cascadingVersion,
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
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"),
  addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
).dependsOn(scaldingArgs, scaldingDate, scaldingSerialization, maple)

lazy val scaldingCommons = module("commons").settings(
  libraryDependencies ++= Seq(
    // TODO: split into scalding-protobuf
    "com.google.protobuf" % "protobuf-java" % protobufVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter.elephantbird" % "elephant-bird-cascading2" % elephantbirdVersion,
    "com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion,
    "com.hadoop.gplcompression" % "hadoop-lzo" % hadoopLzoVersion,
    // TODO: split this out into scalding-thrift
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.thrift" % "libthrift" % thriftVersion,
    // TODO: split this out into a scalding-scrooge
    "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
      exclude("com.google.guava", "guava"),
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
    "junit" % "junit" % junitVersion % "test"
  )
).dependsOn(scaldingArgs, scaldingDate, scaldingCore, scaldingHadoopTest % "test")

lazy val scaldingAvro = module("avro").settings(
  libraryDependencies ++= Seq(
    "cascading.avro" % "avro-scheme" % cascadingAvroVersion,
    "org.apache.avro" % "avro" % avroVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
  )
).dependsOn(scaldingCore)

lazy val scaldingParquetFixtures = module("parquet-fixtures")
   .settings(
     scroogeThriftSourceFolder in Test := baseDirectory.value / "src/test/resources",
     scroogeLanguages in Test := Seq("java", "scala"),
     libraryDependencies ++= Seq(
       "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
         exclude("com.google.guava", "guava"),
       "commons-lang" % "commons-lang" % apacheCommonsVersion, // needed for HashCodeBuilder used in thriftjava
       "org.apache.thrift" % "libthrift" % thriftVersion
     )
   )

lazy val scaldingParquet = module("parquet").settings(
  libraryDependencies ++= Seq(
    "org.apache.parquet" % "parquet-column" % parquetVersion,
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
    "org.apache.parquet" % "parquet-thrift" % parquetVersion
    // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
      exclude("org.apache.parquet", "parquet-pig")
      exclude("com.twitter.elephantbird", "elephant-bird-pig")
      exclude("com.twitter.elephantbird", "elephant-bird-core"),
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.apache.thrift" % "libthrift" % "0.7.0",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.twitter" %% "bijection-macros" % bijectionVersion,
    "com.twitter" %% "chill-bijection" % chillVersion,
    "com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion % "test",
    "org.typelevel" %% "macro-compat" % macroCompatVersion
    ),
  addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full))
  .dependsOn(scaldingCore, scaldingHadoopTest % "test", scaldingParquetFixtures % "test->test")



lazy val scaldingParquetScroogeFixtures = module("parquet-scrooge-fixtures")
  .settings(
    scroogeThriftSourceFolder in Test := baseDirectory.value / "src/test/resources",
    scroogeLanguages in Test := Seq("java", "scala"),
    libraryDependencies ++= Seq(
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
        exclude("com.google.guava", "guava"),
      "commons-lang" % "commons-lang" % apacheCommonsVersion, // needed for HashCodeBuilder used in thriftjava
      "org.apache.thrift" % "libthrift" % thriftVersion
  )
)

lazy val scaldingParquetScrooge = module("parquet-scrooge")
  .settings(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
      "org.apache.parquet" % "parquet-thrift" % parquetVersion % "test" classifier "tests"
        exclude("org.apache.parquet", "parquet-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-core"),
      "com.twitter" %% "scrooge-serializer" % scroogeVersion
        exclude("com.google.guava", "guava"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
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
    libraryDependencies ++= Seq(
      "jline" % "jline" % jlineVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "unprovided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "unprovided"
    ),
    // https://gist.github.com/djspiewak/976cd8ac65e20e136f05
    unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / s"scala-${scalaBinaryVersion(scalaVersion.value)}"
).dependsOn(scaldingCore)
// run with 'unprovided' config includes libs marked 'unprovided' in classpath
.settings(inConfig(Unprovided)(Classpaths.configSettings ++ Seq(
  run := Defaults.runTask(fullClasspath, mainClass in (Runtime, run), runner in (Runtime, run))
)): _*)
.settings(
  // make scalding-repl/run use 'unprovided' config
  run := (run in Unprovided)
)

// zero dependency serialization module
lazy val scaldingSerialization = module("serialization").settings(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value
  ),
addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
)

lazy val scaldingJson = module("json").settings(
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    "org.json4s" %% "json4s-native" % json4SVersion,
    "com.twitter.elephantbird" % "elephant-bird-cascading2" % elephantbirdVersion % "provided"
    )
).dependsOn(scaldingCore)

lazy val scaldingJdbc = module("jdbc").settings(
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "cascading" % "cascading-jdbc-core" % cascadingJDBCVersion,
    "cascading" % "cascading-jdbc-mysql" % cascadingJDBCVersion
  )
).dependsOn(scaldingCore)

lazy val scaldingHadoopTest = module("hadoop-test").settings(
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion classifier "tests",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion classifier "tests",
    "com.twitter" %% "chill-algebird" % chillVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion
  )
).dependsOn(scaldingCore, scaldingSerialization)

// This one uses a different naming convention
lazy val maple = Project(
  id = "maple",
  base = file("maple"),
  settings = sharedSettings
).settings(
  name := "maple",
  mimaPreviousArtifacts := Set.empty,
  crossPaths := false,
  autoScalaLibrary := false,
  // Disable cross publishing for this artifact
  publishArtifact := !scalaVersion.value.startsWith("2.10"),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hbase" % "hbase" % hbaseVersion % "provided",
    "cascading" % "cascading-hadoop" % cascadingVersion
  )
)

lazy val executionTutorial = Project(
  id = "execution-tutorial",
  base = file("tutorial/execution-tutorial"),
  settings = sharedSettings
).settings(
  name := "execution-tutorial",
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "cascading" % "cascading-hadoop" % cascadingVersion
  )
).dependsOn(scaldingCore)

lazy val scaldingDb = module("db").settings(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.twitter" %% "bijection-macros" % bijectionVersion
  ),
  addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
).dependsOn(scaldingCore)

lazy val scaldingThriftMacrosFixtures = module("thrift-macros-fixtures")
  .settings(
    scroogeThriftSourceFolder in Test := baseDirectory.value / "src/test/resources",
    libraryDependencies ++= Seq(
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
        exclude("com.google.guava", "guava"),
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
)

lazy val scaldingThriftMacros = module("thrift-macros")
  .settings(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.twitter" %% "bijection-macros" % bijectionVersion,
    "com.twitter" % "chill-thrift" % chillVersion % "test",
    "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
      exclude("com.google.guava", "guava"),
    "org.apache.thrift" % "libthrift" % thriftVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion  % "test",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion classifier "tests",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion classifier "tests"
  ),
  addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
).dependsOn(
    scaldingCore,
    scaldingHadoopTest % "test",
    scaldingSerialization,
    scaldingThriftMacrosFixtures % "test->test")

def docsSourcesAndProjects(sv: String): Seq[ProjectReference] =
    Seq(
      scaldingArgs,
      scaldingDate,
      scaldingCore
      // scaldingCommons,
      // scaldingAvro,
      // scaldingParquet,
      // scaldingParquetScrooge,
      // scaldingHRaven,
      // scaldingRepl,
      // scaldingJson,
      // scaldingJdbc,
      // scaldingDb,
      // maple,
      // scaldingSerialization,
      // scaldingThriftMacros
    )

lazy val docsMappingsAPIDir = settingKey[String]("Name of subdirectory in site target directory for api docs")

lazy val docSettings = Seq(
  micrositeName := "Scalding",
  micrositeDescription := "Scala API for Cascading.",
  micrositeAuthor := "Scalding's contributors",
  micrositeHighlightTheme := "atom-one-light",
  micrositeHomepage := "http://twitter.github.io/scalding",
  micrositeBaseUrl := "scalding",
  micrositeDocumentationUrl := "api",
  micrositeGithubOwner := "twitter",
  micrositeExtraMdFiles := Map(file("CONTRIBUTING.md") -> "contributing.md"),
  micrositeGithubRepo := "scalding",
    micrositePalette := Map(
    "brand-primary" -> "#5B5988",
    "brand-secondary" -> "#292E53",
    "brand-tertiary" -> "#222749",
    "gray-dark" -> "#49494B",
    "gray" -> "#7B7B7E",
    "gray-light" -> "#E5E5E6",
    "gray-lighter" -> "#F4F3F4",
    "white-color" -> "#FFFFFF"),
  autoAPIMappings := true,
  unidocProjectFilter in (ScalaUnidoc, unidoc) :=
    inProjects(docsSourcesAndProjects(scalaVersion.value):_*),
  docsMappingsAPIDir := "api",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), docsMappingsAPIDir),
  ghpagesNoJekyll := false,
  fork in tut := true,
  fork in (ScalaUnidoc, unidoc) := true,
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url", "https://github.com/twitter/scalding/tree/develop€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-diagrams"
  ),
  git.remoteRepo := "git@github.com:twitter/scalding.git",
  includeFilter in makeSite := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.yml" | "*.md"
  )


// Documentation is generated for projects defined in
// `docsSourcesAndProjects`.
lazy val docs = project
  .enablePlugins(MicrositesPlugin)
  .settings(moduleName := "scalding-docs")
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(unidocSettings)
  .settings(ghpages.settings)
  .settings(docSettings)
  .settings(tutScalacOptions ~= (_.filterNot(Set("-Ywarn-unused-import", "-Ywarn-dead-code"))))
  .dependsOn(scaldingCore)
