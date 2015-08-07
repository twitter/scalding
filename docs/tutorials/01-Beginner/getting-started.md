# Getting Started

## Installation

To get started with Scalding, first clone the [Scalding repository](https://github.com/twitter/scalding) on Github:

```bash
git clone https://github.com/twitter/scalding.git
```

Next, build the code using [sbt](https://github.com/harrah/xsbt/wiki) (a standard Scala build tool). Make sure you have Scala (download [here](http://www.scala-lang.org/downloads), see scalaVersion in project/Build.scala for the correct version to download), and run the following commands:

```bash
./sbt update
./sbt test     # runs the tests; if you do 'sbt assembly' below, these tests, which are long, are repeated
./sbt assembly # creates a fat jar with all dependencies, which is useful when using the scald.rb script
```

Now you're good to go!

## Using Scalding with other versions of Scala

Scalding works with Scala 2.9 and 2.10, though a few configuration files must be changed for this to work. In project/Build.scala, ensure that the proper scalaVersion value is set. Additionally, you'll need to ensure the proper version of specs in the same config. Change the following line

```scala
libraryDependencies += "org.scala-tools.testing" % "specs_2.9.1" % "1.6.9" % "test"
```

to correspond to the proper version of scala (2.9.1 should work with scala 2.9.2). You can find the published versions [here](http://mvnrepository.com/artifact/org.scala-tools.testing).

## IDE Support

Scala's IDE support is generally not as strong as Java's, but there are several options that some people prefer. Both Eclipse and IntelliJ have plugins that support Scala syntax. To generate a project file for Scalding in Eclipse, refer to [this](https://github.com/typesafehub/sbteclipse/) project, and for IntelliJ files, [this](https://github.com/mpeltonen/sbt-idea) (note that with the latter, the 1.1 snapshot is recommended).

