import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val twitterRepo = "com.twitter" at "http://maven.twttr.com"
  val defaultProject = "com.twitter" % "standard-project" % "0.9.25"
  val sbtIdeaRepo = "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
  val sbtIdea = "com.github.mpeltonen" % "sbt-idea-plugin" % "0.3.0"
  val scctRepo = "scct-repo" at "http://mtkopone.github.com/scct/maven-repo/"
  lazy val scctPlugin  = "reaktor" % "sbt-scct-for-2.8" % "0.1-SNAPSHOT"

  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  val assemblySBT = "com.codahale" % "assembly-sbt" % "0.1"
}
