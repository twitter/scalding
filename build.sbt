import ReleaseTransformations._
import scala.collection.JavaConverters._

// Some artifacts can't be resolved in `scalding-hraven` with coursier for some unknown reason.
ThisBuild / useCoursier := false

def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
  case version if version startsWith "2.11" => "2.11"
  case version if version startsWith "2.12" => "2.12"
  case _ => sys.error("unknown error")
}
val algebirdVersion = "0.13.4"
val apacheCommonsVersion = "2.2"
val avroVersion = "1.8.2"
val bijectionVersion = "0.9.5"
val cascadingAvroVersion = "2.1.2"
val catsEffectVersion = "1.1.0"
val catsVersion = "1.5.0"
val chillVersion = "0.8.4"
val dagonVersion = "0.3.1"
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
val scroogeVersion = "18.9.0"
val sparkVersion = "2.4.0"
val slf4jVersion = "1.6.6"
val thriftVersion = "0.9.3"
val junitVersion = "4.10"
val jlineVersion = "2.14.3"

val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")

val mimaSettings = Seq(
    mimaPreviousArtifacts := youngestForwardCompatible(name.value)
)

val sharedSettings = Seq(
  organization := "com.twitter",

  scalaVersion := "2.11.12",

  crossScalaVersions := Seq(scalaVersion.value, "2.12.8"),

  javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

  javacOptions in doc := Seq("-source", "1.6"),

  wartremoverErrors in (Compile, compile) ++= Seq(
    Wart.OptionPartial, Wart.ExplicitImplicitTypes, Wart.LeakingSealed,
    Wart.Return, Wart.EitherProjectionPartial),

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
    "Concurrent Maven Repo" at "https://conjars.org/repo",
    "Twitter Maven" at "https://maven.twttr.com",
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
      "-language:existentials",
      "-Ywarn-unused-import"
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
  assemblyExcludedJars in assembly := {
    val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
      (fullClasspath in assembly).value filter {
        jar => excludes(jar.data.getName)
      }
  },
  // Some of these files have duplicates, let's ignore:
  assemblyMergeStrategy in assembly :=  {
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
    case x => (assemblyMergeStrategy in assembly).value(x)
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
)

lazy val scalding = project
  .in(file("."))
  .settings(
    sharedSettings,
    noPublishSettings
  )
  .aggregate(
    `scalding-args`,
    `scalding-date`,
    `scalding-quotation`,
    `scalding-cats`,
    `scalding-core`,
    `scalding-commons`,
    `scalding-avro`,
    `scalding-parquet`,
    `scalding-parquet-scrooge`,
    `scalding-hraven`,
    `scalding-repl`,
    `scalding-json`,
    `scalding-jdbc`,
    `scalding-hadoop-test`,
    `scalding-estimators-test`,
    `scalding-db`,
    maple,
    `execution-tutorial`,
    `scalding-serialization`,
    `scalding-spark`,
    `scalding-thrift-macros`
 )

lazy val `scalding-assembly` = project
  .in(file("assembly"))
  .settings(
    sharedSettings,
    noPublishSettings
  )
  .aggregate(
    `scalding-args`,
    `scalding-date`,
    `scalding-quotation`,
    `scalding-core`,
    `scalding-commons`,
    `scalding-avro`,
    `scalding-parquet`,
    `scalding-parquet-scrooge`,
    `scalding-hraven`,
    `scalding-repl`,
    `scalding-json`,
    `scalding-jdbc`,
    maple,
    `scalding-serialization`
  )

lazy val noPublishSettings = Seq(
    skip in publish := true
  )

/**
 * This returns the youngest jar we released that is compatible with
 * the current.
 */
val ignoredModules = Set[String]("benchmarks")

def youngestForwardCompatible(subProj: String): Set[ModuleID] =
  Set.empty
// Enable mima binary check back after releasing 0.18.0
//  Some(subProj)
//    .filterNot(ignoredModules.contains(_))
//    .map {
//    s => "com.twitter" %% (s"scalding-$s") % "0.17.0"
//  }


lazy val `scalding-args` = project
    .in(file("scalding-args"))
    .settings(
        name := "scalding-args",
        sharedSettings,
        mimaSettings,
    )

lazy val `scalding-date` = project
    .in(file("scalding-date"))
    .settings(
        name := "scalding-date",
        sharedSettings,
        mimaSettings,
    )

lazy val cascadingVersion =
  System.getenv.asScala.getOrElse("SCALDING_CASCADING_VERSION", "2.6.1")

lazy val cascadingJDBCVersion =
  System.getenv.asScala.getOrElse("SCALDING_CASCADING_JDBC_VERSION", "2.6.0")

lazy val `scalding-benchmarks` = project
  .in(file("scalding-benchmarks"))
  .settings(
    name := "scalding-benchmarks",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
        "com.storm-enroute" %% "scalameter" % scalameterVersion % "test",
        "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
      ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Test := false
  ).dependsOn(`scalding-core`)

lazy val `scalding-quotation` = project
  .in(file("scalding-quotation"))
  .settings(
    name := "scalding-quotation",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided",
      "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided"
    )
  )

lazy val `scalding-core` = project
  .in(file("scalding-core"))
  .dependsOn(`scalding-args`, `scalding-date`, `scalding-serialization`, maple, `scalding-quotation`)
  .settings(
    name := "scalding-core",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "cascading" % "cascading-core" % cascadingVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion,
      "cascading" % "cascading-local" % cascadingVersion,
      "com.stripe" %% "dagon-core" % dagonVersion,
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
  )

lazy val `scalding-cats` = project
  .in(file("scalding-cats"))
  .dependsOn(`scalding-args`, `scalding-date`, `scalding-core`)
  .settings(
    name := "scalding-cats",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-laws" % catsVersion % "test",
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "cats-effect-laws" % catsEffectVersion % "test"
    )
  )


lazy val `scalding-spark` = project
  .in(file("scalding-spark"))
  .dependsOn(`scalding-core`)
  .settings(
    name := "scalding-spark",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )

lazy val `scalding-commons` = project
  .in(file("scalding-commons"))
  .dependsOn(`scalding-args`, `scalding-date`, `scalding-core`, `scalding-hadoop-test` % "test")
  .settings(
    name := "scalding-commons",
    sharedSettings,
    mimaSettings,
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
  )

lazy val `scalding-avro` = project
  .in(file("scalding-avro"))
  .dependsOn(`scalding-core`)
  .settings(
    name := "scalding-avro",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "cascading.avro" % "avro-scheme" % cascadingAvroVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    )
  )

lazy val `scalding-parquet-fixtures` = project
  .in(file("scalding-parquet-fixtures"))
  .settings(
    name := "scalding-parquet-fixtures",
    sharedSettings,
    mimaSettings,
    scroogeThriftSourceFolder in Test := baseDirectory.value / "src/test/resources",
    scroogeLanguages in Test := Seq("java", "scala"),
    libraryDependencies ++= Seq(
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
        exclude("com.google.guava", "guava"),
      "commons-lang" % "commons-lang" % apacheCommonsVersion, // needed for HashCodeBuilder used in thriftjava
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
  )

lazy val `scalding-parquet` = project
  .in(file("scalding-parquet"))
  .dependsOn(`scalding-core`, `scalding-hadoop-test` % "test", `scalding-parquet-fixtures` % "test->test")
  .settings(
    name := "scalding-parquet",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.apache.parquet" % "parquet-thrift" % parquetVersion
      // see https://issues.apache.org/jira/browse/PARQUET-143 for exclusions
        exclude("org.apache.parquet", "parquet-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-pig")
        exclude("com.twitter.elephantbird", "elephant-bird-core"),
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.apache.thrift" % "libthrift" % thriftVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "com.twitter" %% "bijection-macros" % bijectionVersion,
      "com.twitter" %% "chill-bijection" % chillVersion,
      "com.twitter.elephantbird" % "elephant-bird-core" % elephantbirdVersion % "test"
      ),
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  )



lazy val `scalding-parquet-scrooge-fixtures` = project
  .in(file("scalding-parquet-scrooge-fixtures"))
  .settings(
    name := "scalding-parquet-scrooge-fixtures",
    sharedSettings,
    mimaSettings,
    scroogeThriftSourceFolder in Test := baseDirectory.value / "src/test/resources",
    scroogeLanguages in Test := Seq("java", "scala"),
    libraryDependencies ++= Seq(
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
        exclude("com.google.guava", "guava"),
      "commons-lang" % "commons-lang" % apacheCommonsVersion, // needed for HashCodeBuilder used in thriftjava
      "org.apache.thrift" % "libthrift" % thriftVersion
  )
)

lazy val `scalding-parquet-scrooge` = project
  .in(file("scalding-parquet-scrooge"))
  .dependsOn(`scalding-core`, `scalding-parquet` % "compile->compile;test->test",
  `scalding-parquet-scrooge-fixtures` % "test->test")
  .settings(
    name := "scalding-parquet-scrooge",
    sharedSettings,
    mimaSettings,
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
  )

lazy val `scalding-hraven` = project
  .in(file("scalding-hraven"))
  .dependsOn(`scalding-core`)
  .settings(
    name := "scalding-hraven",
    sharedSettings,
    mimaSettings,
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
      "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
      "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    )
  )

// create new configuration which will hold libs otherwise marked as 'provided'
// so that we can re-include them in 'run'. unfortunately, we still have to
// explicitly add them to both 'provided' and 'unprovided', as below
// solution borrowed from: http://stackoverflow.com/a/18839656/1404395
val Unprovided = config("unprovided") extend Runtime

lazy val `scalding-repl` = project
  .in(file("scalding-repl"))
  .dependsOn(`scalding-core`)
  .configs(Unprovided) // include 'unprovided' as config option
  .settings(
    name := "scalding-repl",
    sharedSettings,
    mimaSettings,
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
    // run with 'unprovided' config includes libs marked 'unprovided' in classpath
    inConfig(Unprovided)(Classpaths.configSettings ++ Seq(
      run := Defaults.runTask(fullClasspath, mainClass in (Runtime, run), runner in (Runtime, run))
    )),
    // make scalding-repl/run use 'unprovided' config
    run := run in Unprovided
  )

// zero dependency serialization module
lazy val `scalding-serialization` = project
  .in(file("scalding-serialization"))
  .settings(
    name := "scalding-serialization",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  )

lazy val `scalding-json` = project
  .in(file("scalding-json"))
  .dependsOn(`scalding-core`)
  .settings(
    name := "scalding-json",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.json4s" %% "json4s-native" % json4SVersion,
      "com.twitter.elephantbird" % "elephant-bird-cascading2" % elephantbirdVersion % "provided"
    )
  )

lazy val `scalding-jdbc` = project
  .in(file("scalding-jdbc"))
  .dependsOn(`scalding-core`)
  .settings(
    name := "scalding-jdbc",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "cascading" % "cascading-jdbc-core" % cascadingJDBCVersion,
      "cascading" % "cascading-jdbc-mysql" % cascadingJDBCVersion
    )
  )

lazy val `scalding-hadoop-test` = project
  .in(file("scalding-hadoop-test"))
  .dependsOn(`scalding-core`, `scalding-serialization`)
  .settings(
    name := "scalding-hadoop-test",
    sharedSettings,
    mimaSettings,
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
  )

lazy val `scalding-estimators-test` = project
  .in(file("scalding-estimators-test"))
  .dependsOn(`scalding-hadoop-test` % "test")
  .settings(
    name := "scalding-estimators-test",
    sharedSettings,
    mimaSettings,
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
  )

// This one uses a different naming convention
lazy val maple = project
  .in(file("maple"))
  .settings(
    name := "maple",
    sharedSettings,
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

lazy val `execution-tutorial` = project
  .in(file("tutorial") / "execution-tutorial")
  .dependsOn(`scalding-core`)
  .settings(
    name := "execution-tutorial",
    sharedSettings,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "cascading" % "cascading-hadoop" % cascadingVersion
    )
  )

lazy val `scalding-db` = project
  .in(file("scalding-db"))
  .dependsOn(`scalding-core`)
  .settings(
    name := "scalding-db",
    sharedSettings,
    mimaSettings,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "com.twitter" %% "bijection-macros" % bijectionVersion
    ),
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  )

lazy val `scalding-thrift-macros-fixtures` = project
  .in(file("scalding-thrift-macros-fixtures"))
  .settings(
    name := "scalding-thrift-macros-fixtures",
    sharedSettings,
    mimaSettings,
    scroogeThriftSourceFolder in Test := baseDirectory.value / "src/test/resources",
    libraryDependencies ++= Seq(
      "com.twitter" %% "scrooge-serializer" % scroogeVersion % "provided"
        exclude("com.google.guava", "guava"),
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
)

lazy val `scalding-thrift-macros` = project
  .in(file("scalding-thrift-macros"))
  .dependsOn(
     `scalding-core`,
     `scalding-hadoop-test` % "test",
     `scalding-serialization`,
     `scalding-thrift-macros-fixtures` % "test->test")
  .settings(
    name := "scalding-thrift-macros",
    sharedSettings,
    mimaSettings,
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
  )

def docsSourcesAndProjects(sv: String): Seq[ProjectReference] =
    Seq(
      `scalding-args`,
      `scalding-date`,
      `scalding-core`
      // `scalding-commons`,
      // `scalding-avro`,
      // `scalding-parquet`,
      // `scalding-parquet-scrooge`,
      // `scalding-hraven`,
      // `scalding-repl`,
      // `scalding-json`,
      // `scalding-jdbc`,
      // `scalding-db`,
      // maple,
      // `scalding-serialization`,
      // `scalding-thrift-macros`
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
  micrositeCompilingDocsTool := WithTut,
  micrositeExtraMdFiles := Map(file("CONTRIBUTING.md") -> microsites.ExtraMdFileConfig("contributing.md", "page")),
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
  .enablePlugins(MicrositesPlugin, ScalaUnidocPlugin, GhpagesPlugin)
  .settings(moduleName := "scalding-docs")
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(docSettings)
  .settings(scalacOptions in Tut ~= (_.filterNot(Set("-Ywarn-unused-import", "-Ywarn-dead-code"))))
  .dependsOn(`scalding-core`)
