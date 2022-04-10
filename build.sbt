import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import scala.collection.JavaConverters._
import microsites.ExtraMdFileConfig

def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
  case version if version.startsWith("2.11") => "2.11"
  case version if version.startsWith("2.12") => "2.12"
  case _                                     => sys.error("unknown error")
}
val algebirdVersion = "0.13.4"
val apacheCommonsVersion = "2.2"
val avroVersion = "1.8.2"
val bijectionVersion = "0.9.5"
val cascadingAvroVersion = "2.1.2"
val catsEffectVersion = "1.1.0"
val catsVersion = "1.5.0"
val chillVersion = "0.8.4"
val elephantbirdVersion = "4.15"
val hadoopLzoVersion = "0.4.19"
val hadoopVersion = "2.5.0"
val hbaseVersion = "1.2.4"
val hravenVersion = "1.0.1"
val jacksonVersion = "2.8.7"
val json4SVersion = "3.5.0"
val paradiseVersion = "2.1.1"
val parquetVersion = "1.10.0"
val protobufVersion = "2.4.1"
val scalameterVersion = "0.8.2"
val scalaCheckVersion = "1.13.4"
val scalaTestVersion = "3.0.1"
val scroogeVersion = "19.8.0"
val sparkVersion = "2.4.0"
val beamVersion = "2.29.0"
val slf4jVersion = "1.7.30"
val thriftVersion = "0.9.3"
val junitVersion = "4.10"
val jlineVersion = "2.14.3"

val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

// these are override functions for sbt-dynver (plugin for resolving project version from git tags)
// the default behaviour includes timestamps in SNAPSHOT versions which is incompatible with tests and unnecessary
// implementation based on source in https://github.com/sbt/sbt-dynver/blob/master/dynver/src/main/scala/sbtdynver/DynVer.scala
def versionFmt(out: sbtdynver.GitDescribeOutput): String = {
  // head commit has a version tag, in this case we know that we have a final release
  // we should publish in the form of <VERSION>
  if (out.commitSuffix.isEmpty) {
    return out.ref.dropPrefix
  }
  // head commit has no tag (ie. PR has been merged into develop)
  // we should publish in the form of <latest VERSION>-<commit SHA>-SNAPSHOT
  out.ref.dropPrefix + out.commitSuffix.mkString("-", "-", "") + "-SNAPSHOT"
}
// this edge case is not relevant as it is only triggered on non-git repos
def fallbackVersion(d: java.util.Date): String = "HEAD"

val sharedSettings = Seq(
  version := dynverGitDescribeOutput.value.mkVersion(versionFmt, fallbackVersion(dynverCurrentDate.value)),
  dynver := dynverGitDescribeOutput.value.mkVersion(versionFmt, fallbackVersion(dynverCurrentDate.value)),
  organization := "com.twitter",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.14"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  doc / javacOptions := Seq("-source", "1.8"),
  versionScheme := Some("early-semver"),
  Compile / compile / wartremoverErrors ++= Seq(
    //Wart.OptionPartial, // this kills the ability to use serialization macros
    Wart.ExplicitImplicitTypes,
    Wart.LeakingSealed,
    Wart.Return,
    Wart.EitherProjectionPartial
  ),
  libraryDependencies ++= Seq(
    "org.mockito" % "mockito-all" % "1.8.5" % "test",
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
    "com.novocode" % "junit-interface" % "0.10" % "test"
  ),
  resolvers ++= Seq(
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases,
    "Concurrent Maven Repo".at("https://conjars.org/repo"),
    "Twitter Maven".at("https://maven.twttr.com"),
    "Cloudera".at("https://repository.cloudera.com/artifactory/cloudera-repos/")
  ),
  printDependencyClasspath := {
    val cp = (Compile / dependencyClasspath).value
    cp.foreach(f => println(s"${f.metadata.get(moduleID.key)} => ${f.data}"))
  },
  Test / fork := true,
  updateOptions := updateOptions.value.withCachedResolution(true),
  update / aggregate := false,
  Test / javaOptions ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m"),
  Global / concurrentRestrictions := Seq(
    Tags.limitAll(1)
  ),
  Test / parallelExecution := false,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials",
    "-Ywarn-unused-import"
  ),
  Compile / doc / scalacOptions ++= Seq(scalaVersion.value).flatMap {
    case v if v.startsWith("2.12") => Seq("-no-java-comments") //workaround for scala/scala-dev#249
    case _                         => Seq()
  },

  // Code coverage options
  jacocoReportSettings := JacocoReportSettings(
    "Jacoco Coverage Report",
    None,
    JacocoThresholds(),
    Seq(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML),
    "utf-8"),

  // Enables full stack traces in scalatest
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),

  // Uncomment if you don't want to run all the tests before building assembly
  // test in assembly := {},
  assembly / logLevel := Level.Warn,

  // Publishing options:
  Test / publishArtifact := false,
  pomIncludeRepository := { x => false },

  // Janino includes a broken signature, and is not needed:
  assembly / assemblyExcludedJars := {
    val excludes =
      Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar", "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
    (assembly / fullClasspath).value.filter { jar =>
      excludes(jar.data.getName)
    }
  },
  // Some of these files have duplicates, let's ignore:
  assembly / assemblyMergeStrategy := {
    case s if s.endsWith(".class")         => MergeStrategy.last
    case s if s.endsWith("project.clj")    => MergeStrategy.concat
    case s if s.endsWith(".html")          => MergeStrategy.last
    case s if s.endsWith(".dtd")           => MergeStrategy.last
    case s if s.endsWith(".xsd")           => MergeStrategy.last
    case s if s.endsWith("pom.properties") => MergeStrategy.last
    case s if s.endsWith("pom.xml")        => MergeStrategy.last
    case s if s.endsWith(".jnilib")        => MergeStrategy.rename
    case s if s.endsWith("jansi.dll")      => MergeStrategy.rename
    case s if s.endsWith("libjansi.so")    => MergeStrategy.rename
    case s if s.endsWith("properties")     => MergeStrategy.filterDistinctLines
    case x                                 => (assembly / assemblyMergeStrategy).value(x)
  },
  pomExtra := (<url>https://github.com/twitter/scalding</url>
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
          <url>https://twitter.com/posco</url>
        </developer>
        <developer>
          <id>avibryant</id>
          <name>Avi Bryant</name>
          <url>https://twitter.com/avibryant</url>
        </developer>
        <developer>
          <id>argyris</id>
          <name>Argyris Zymnis</name>
          <url>https://twitter.com/argyris</url>
        </developer>
      </developers>)
) ++ mimaDefaultSettings

lazy val scalding = Project(id = "scalding", base = file("."))
  .settings(sharedSettings ++ noPublishSettings)
  .aggregate(
    scaldingArgs,
    scaldingDate,
    scaldingQuotation,
    scaldingCats,
    scaldingDagon,
    scaldingCore,
    scaldingCommons,
    scaldingAvro,
    scaldingParquet,
    scaldingParquetScrooge,
    scaldingHRaven,
    scaldingRepl,
    scaldingJson,
    scaldingHadoopTest,
    scaldingEstimatorsTest,
    scaldingDb,
    maple,
    executionTutorial,
    scaldingSerialization,
    scaldingSpark,
    scaldingBeam,
    scaldingThriftMacros
  )

lazy val scaldingAssembly = Project(id = "scalding-assembly", base = file("assembly"))
  .settings(sharedSettings ++ noPublishSettings)
  .aggregate(
    scaldingArgs,
    scaldingDate,
    scaldingQuotation,
    scaldingCore,
    scaldingCommons,
    scaldingAvro,
    scaldingParquet,
    scaldingParquetScrooge,
    scaldingHRaven,
    scaldingRepl,
    scaldingJson,
    maple,
    scaldingSerialization
  )

lazy val noPublishSettings = Seq(
  publish := (()),
  publishLocal := (()),
  test := (()),
  publishArtifact := false
)

/**
 * This returns the youngest jar we released that is compatible with the current.
 */
val ignoredModules = Set[String]("benchmarks")

def youngestForwardCompatible(subProj: String) =
  None
// Enable mima binary check back after releasing 0.18.0
//  Some(subProj)
//    .filterNot(ignoredModules.contains(_))
//    .map {
//    s => "com.twitter" %% (s"scalding-$s") % "0.17.0"
//  }

def module(name: String) = {
  val id = "scalding-%s".format(name)
  Project(id = id, base = file(id)).settings(
    sharedSettings ++ Seq(Keys.name := id, mimaPreviousArtifacts := youngestForwardCompatible(name).toSet)
  )
}

lazy val scaldingArgs = module("args")

lazy val scaldingDate = module("date")

lazy val cascadingVersion =
  System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.6.1")

lazy val scaldingBenchmarks = module("benchmarks")
  .settings(
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % scalameterVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    Test / parallelExecution := false
  )
  .dependsOn(scaldingCore)

lazy val scaldingQuotation = module("quotation").settings(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided",
    "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided"
  )
)

lazy val scaldingDagon = module("dagon").settings(
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.0" cross CrossVersion.full),
  Compile / unmanagedSourceDirectories ++= scaldingDagonSettings.scalaVersionSpecificFolders("main", baseDirectory.value, scalaVersion.value),
  Test / unmanagedSourceDirectories ++= scaldingDagonSettings.scalaVersionSpecificFolders("test", baseDirectory.value, scalaVersion.value),
)

lazy val scaldingBase = module("base")
  .settings(
    libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion
    ),
    // buildInfo here refers to https://github.com/sbt/sbt-buildinfo
    // for logging purposes, src/main/scala/com/twitter/package.scala would like to know the scalding-version
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.twitter.scalding", // the codegen would be under com.twitter.scalding.BuildInfo
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(scaldingArgs, scaldingDagon, scaldingSerialization)


lazy val scaldingCore = module("core")
  .settings(
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
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    ),
    addCompilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full))
  )
  .dependsOn(scaldingArgs, scaldingBase, scaldingDate, scaldingSerialization, maple, scaldingQuotation, scaldingDagon)

lazy val scaldingCats = module("cats")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-laws" % catsVersion % "test",
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "cats-effect-laws" % catsEffectVersion % "test"
    )
  )
  .dependsOn(scaldingArgs, scaldingDate, scaldingCore)

lazy val scaldingSpark = module("spark")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )
  .dependsOn(scaldingCore)

lazy val scaldingBeam = module("beam")
  .settings(
    libraryDependencies ++= Seq(
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "chill" % chillVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % "test",
      // Including this dependency since scalding configuration depends on hadoop
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    ),
    // Useful for the BeamPlanner implementation so we know if anything is missing
    scalacOptions ++= Seq("-Ypatmat-exhaust-depth", "200")
  )
  .dependsOn(scaldingCore)

lazy val scaldingCommons = module("commons")
  .settings(
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
      ("com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided")
        .exclude("com.google.guava", "guava"),
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
      "junit" % "junit" % junitVersion % "test"
    )
  )
  .dependsOn(scaldingArgs, scaldingDate, scaldingCore, scaldingHadoopTest % "test")

lazy val scaldingAvro = module("avro")
  .settings(
    libraryDependencies ++= Seq(
      "cascading.avro" % "avro-scheme" % cascadingAvroVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    )
  )
  .dependsOn(scaldingCore)

lazy val scaldingParquetFixtures = module("parquet-fixtures")
  .settings(
    Test / scroogeThriftSourceFolder := baseDirectory.value / "src/test/resources",
    Test / scroogeLanguages := Seq("java", "scala"),
    libraryDependencies ++= Seq(
      ("com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided")
        .exclude("com.google.guava", "guava"),
      "commons-lang" % "commons-lang" % apacheCommonsVersion, // needed for HashCodeBuilder used in thriftjava
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
  )

lazy val scaldingParquet = module("parquet")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      ("org.apache.parquet" % "parquet-thrift" % parquetVersion)
        // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
        .exclude("org.apache.parquet", "parquet-pig")
        .exclude("com.twitter.elephantbird", "elephant-bird-pig")
        .exclude("com.twitter.elephantbird", "elephant-bird-core"),
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.apache.thrift" % "libthrift" % thriftVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "com.twitter" %% "bijection-macros" % bijectionVersion,
      "com.twitter" %% "chill-bijection" % chillVersion,
      "com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion % "test"
    ),
    addCompilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full))
  )
  .dependsOn(scaldingCore, scaldingHadoopTest % "test", scaldingParquetFixtures % "test->test")

lazy val scaldingParquetScroogeFixtures = module("parquet-scrooge-fixtures")
  .settings(
    Test / scroogeThriftSourceFolder := baseDirectory.value / "src/test/resources",
    Test / scroogeLanguages := Seq("java", "scala"),
    libraryDependencies ++= Seq(
      ("com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided")
        .exclude("com.google.guava", "guava"),
      "commons-lang" % "commons-lang" % apacheCommonsVersion, // needed for HashCodeBuilder used in thriftjava
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
  )

lazy val scaldingParquetScrooge = module("parquet-scrooge")
  .settings(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
      ("org.apache.parquet" % "parquet-thrift" % parquetVersion % "test")
        .classifier("tests")
        .exclude("org.apache.parquet", "parquet-pig")
        .exclude("com.twitter.elephantbird", "elephant-bird-pig")
        .exclude("com.twitter.elephantbird", "elephant-bird-core"),
      ("com.twitter" %% "scrooge-serializer" % scroogeVersion)
        .exclude("com.google.guava", "guava"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "junit" % "junit" % junitVersion % "test"
    )
  )
  .dependsOn(
    scaldingCore,
    scaldingParquet % "compile->compile;test->test",
    scaldingParquetScroogeFixtures % "test->test"
  )

lazy val scaldingHRaven = module("hraven")
  .settings(
    libraryDependencies ++= Seq(
      ("com.twitter.hraven" % "hraven-core" % hravenVersion)
        // These transitive dependencies cause sbt to give a ResolveException
        // because they're not available on Maven. We don't need them anyway.
        // See https://github.com/twitter/cassie/issues/13
        .exclude("javax.jms", "jms")
        .exclude("com.sun.jdmk", "jmxtools")
        .exclude("com.sun.jmx", "jmxri")

        // These transitive dependencies of hRaven cause conflicts when
        // running scalding-hraven/*assembly and aren't needed
        // for the part of the hRaven API that we use anyway
        .exclude("com.twitter.common", "application-module-log")
        .exclude("com.twitter.common", "application-module-stats")
        .exclude("com.twitter.common", "args")
        .exclude("com.twitter.common", "application")
        // Excluding this dependencies because they get resolved to incorrect version,
        // and not needed during compilation.
        .exclude("com.twitter", "util-registry_2.10")
        .exclude("com.twitter", "util-core_2.10")
        .exclude("com.twitter", "util-jvm_2.10"),
      "org.apache.hbase" % "hbase" % hbaseVersion,
      "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    )
  )
  .dependsOn(scaldingCore)

lazy val scaldingRepl = module("repl")
  .settings(
    console / initialCommands := """
      import com.twitter.scalding._
      import com.twitter.scalding.ReplImplicits._
      import com.twitter.scalding.ReplImplicitContext._
      """,
    libraryDependencies ++= Seq(
      "jline" % "jline" % jlineVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    )
  )
  .dependsOn(scaldingCore)
  .settings(
    inConfig(Compile)(
      Classpaths.configSettings ++ Seq(
        // This is needed to make "provided" dependencies presented in repl,
        // solution borrowed from: http://stackoverflow.com/a/18839656/1404395
        run := Defaults
          .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
          .evaluated,
        // we need to fork repl task, because scala repl doesn't work well with sbt classloaders.
        run / fork := true,
        run / connectInput := true,
        run / outputStrategy := Some(OutputStrategy.StdoutOutput)
      )
    ): _*
  )

// zero dependency serialization module
lazy val scaldingSerialization = module("serialization").settings(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value
  ),
  addCompilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full))
)

lazy val scaldingJson = module("json")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.json4s" %% "json4s-native" % json4SVersion,
      "com.twitter.elephantbird" % "elephant-bird-cascading2" % elephantbirdVersion % "provided"
    )
  )
  .dependsOn(scaldingCore)

lazy val scaldingHadoopTest = module("hadoop-test")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion,
      ("org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion).classifier("tests"),
      "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion,
      ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion).classifier("tests"),
      ("org.apache.hadoop" % "hadoop-common" % hadoopVersion).classifier("tests"),
      ("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion).classifier("tests"),
      "com.twitter" %% "chill-algebird" % chillVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    )
  )
  .dependsOn(scaldingCore, scaldingSerialization)

lazy val scaldingEstimatorsTest = module("estimators-test")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion,
      ("org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion).classifier("tests"),
      "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion,
      ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion).classifier("tests"),
      ("org.apache.hadoop" % "hadoop-common" % hadoopVersion).classifier("tests"),
      ("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion).classifier("tests"),
      "com.twitter" %% "chill-algebird" % chillVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    )
  )
  .dependsOn(scaldingHadoopTest % "test")

// This one uses a different naming convention
lazy val maple = Project(
  id = "maple",
  base = file("maple")
).settings(
  sharedSettings ++ Seq(
    name := "maple",
    mimaPreviousArtifacts := Set.empty,
    crossPaths := false,
    autoScalaLibrary := false,
    publishArtifact := true,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.hbase" % "hbase" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
  )
)

lazy val executionTutorial = Project(
  id = "execution-tutorial",
  base = file("tutorial/execution-tutorial")
).settings(
  sharedSettings ++ Seq(
    name := "execution-tutorial",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
  )
).dependsOn(scaldingCore)

lazy val scaldingDb = module("db")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "com.twitter" %% "bijection-macros" % bijectionVersion
    ),
    addCompilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full))
  )
  .dependsOn(scaldingCore)

lazy val scaldingThriftMacrosFixtures = module("thrift-macros-fixtures")
  .settings(
    Test / scroogeThriftSourceFolder := baseDirectory.value / "src/test/resources",
    libraryDependencies ++= Seq(
      ("com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided")
        .exclude("com.google.guava", "guava"),
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
  )

lazy val scaldingThriftMacros = module("thrift-macros")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "com.twitter" %% "bijection-macros" % bijectionVersion,
      "com.twitter" % "chill-thrift" % chillVersion % "test",
      ("com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided")
        .exclude("com.google.guava", "guava"),
      "org.apache.thrift" % "libthrift" % thriftVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "test",
      ("org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion).classifier("tests"),
      "org.apache.hadoop" % "hadoop-yarn-server" % hadoopVersion % "test",
      ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion).classifier("tests"),
      ("org.apache.hadoop" % "hadoop-common" % hadoopVersion).classifier("tests"),
      ("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion).classifier("tests")
    ),
    addCompilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full))
  )
  .dependsOn(
    scaldingCore,
    scaldingHadoopTest % "test",
    scaldingSerialization,
    scaldingThriftMacrosFixtures % "test->test"
  )

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
  micrositeHomepage := "https://twitter.github.io/scalding",
  micrositeBaseUrl := "scalding",
  micrositeDocumentationUrl := "api",
  micrositeGithubOwner := "twitter",
  micrositeExtraMdFiles := Map(file("CONTRIBUTING.md") -> ExtraMdFileConfig("contributing.md", "home")),
  micrositeGithubRepo := "scalding",
  micrositePalette := Map(
    "brand-primary" -> "#5B5988",
    "brand-secondary" -> "#292E53",
    "brand-tertiary" -> "#222749",
    "gray-dark" -> "#49494B",
    "gray" -> "#7B7B7E",
    "gray-light" -> "#E5E5E6",
    "gray-lighter" -> "#F4F3F4",
    "white-color" -> "#FFFFFF"
  ),
  autoAPIMappings := true,
  ScalaUnidoc / unidoc / unidocProjectFilter :=
    inProjects(docsSourcesAndProjects(scalaVersion.value): _*),
  docsMappingsAPIDir := "api",
  addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, docsMappingsAPIDir),
  ghpagesNoJekyll := false,
  ScalaUnidoc / unidoc / fork := true,
  ScalaUnidoc / unidoc / scalacOptions ++= Seq(
    "-doc-source-url",
    "https://github.com/twitter/scalding/tree/develop€{FILE_PATH}.scala",
    "-sourcepath",
    (LocalRootProject / baseDirectory).value.getAbsolutePath,
    "-diagrams"
  ),
  mdocIn := new File((LocalRootProject / baseDirectory).value, "docs/src"),
  git.remoteRepo := "git@github.com:twitter/scalding.git",
  makeSite / includeFilter := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.yml" | "*.md"
)

// Documentation is generated for projects defined in
// `docsSourcesAndProjects`.
lazy val docs = project
  .enablePlugins(MdocPlugin)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(ScalaUnidocPlugin)
  .enablePlugins(GhpagesPlugin)
  .settings(moduleName := "scalding-docs")
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(docSettings)
  .settings(Compile / scalacOptions ~= (_.filterNot(Set("-Ywarn-unused-import", "-Ywarn-dead-code"))))
  .dependsOn(scaldingCore)
