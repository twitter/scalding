import sbt._
import com.twitter.sbt._
import reaktor.scct.ScctProject

class Project(info: ProjectInfo) extends StandardProject(info)
  with PackageDist
  with IdeaProject
  with assembly.AssemblyBuilder
  with ScctProject {

  override def distZipName = "%s.zip".format(name)

  //Setup the cascading maven repo:
  val conjars = "Concurrent Maven Repo" at "http://conjars.org/repo"
  val c2core   = "cascading" % "cascading-core" % "2.0.0-wip-165"
  val c2local  = "cascading" % "cascading-local" % "2.0.0-wip-165"
  val c2hadoop = "cascading" % "cascading-hadoop" % "2.0.0-wip-165"

  val hadoop   = "org.apache.hadoop" % "hadoop-core" % "0.20.2"
  val commons  = "commons-lang" % "commons-lang" % "2.4"
  val kryo     = "de.javakaffee" % "kryo-serializers" % "0.9"

  // Testing
  val specs = "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5" % "test"
}
