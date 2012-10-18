import sbt._

object ScaldingBuild extends Build {
  lazy val root = Project("root", file("."))
                    .dependsOn(RootProject(uri("git://github.com/twitter/algebird.git")))
                    .dependsOn(RootProject(uri("git://github.com/twitter/chill.git")))
}