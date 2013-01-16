import sbt._

object ScaldingBuild extends Build {
  lazy val root = Project("root", file("."))
// This caused us some pain:
//                    .dependsOn(RootProject(uri("git://github.com/twitter/algebird.git#master")))
//                    .dependsOn(RootProject(uri("git://github.com/twitter/chill.git#master")))
}
