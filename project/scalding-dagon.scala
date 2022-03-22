import sbt.CrossVersion

import java.io.File
import java.nio.file.Paths

object scaldingDagonSettings {

  // load either scala-2.12- or scala-2.12+ dagon src depending on scala version
  def scalaVersionSpecificFolders(srcName: String, srcBaseDir: File, scalaVersion: String) = {

    def extraDirs(suffix: String) = {
      val scalaCompat = Paths.get(srcBaseDir.toString)
        .resolve("src")
        .resolve(srcName)
        .resolve("scala" + suffix)
        .toFile
      Seq(scalaCompat)
    }

    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, y)) if y <= 12 =>
        extraDirs("-2.12-")
      case Some((2, y)) if y >= 13 =>
        extraDirs("-2.13+")
      case _ => Nil
    }
  }

}
