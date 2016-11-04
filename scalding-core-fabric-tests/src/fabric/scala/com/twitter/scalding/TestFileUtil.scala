package com.twitter.scalding

import java.io.File

/**
 * Miscellaneous bits of code related to checking whether the expected
 * output files are within a directory, etc.
 */
object TestFileUtil {
  import scala.language.implicitConversions

  case class RichDirectory(dir: File) {
    import RichDirectory._
    lazy val allFiles = dir.listFiles.toSet

    lazy val rawFileNames = allFiles.map(_.getName)

    lazy val fileNameSet = rawFileNames.filterNot(isFileNameOfCrcFile)

    def fileNameSetExSuccess = fileNameSet - "_SUCCESS"

    /* The naming convention of the parts is a fabric-specific implementation detail. However,
     * they tend to start with part- */
    def partFiles = fileNameSet.filter(_.startsWith("part-"))

    def list = dir.list.toSet
    def listFiles = allFiles

    implicit def backToFile = dir
    override def toString = dir.toString
  }
  implicit def toRichDirectory(dir: File) = RichDirectory(dir)

  object RichDirectory {
    def apply(dirname: String): RichDirectory = RichDirectory(new File(dirname))
    def apply(parent: File, dirname: String): RichDirectory = RichDirectory(new File(parent, dirname))
    def apply(parent: RichDirectory, dirname: String): RichDirectory = RichDirectory(new File(parent.dir, dirname))

    private val CrcPattern = "^[.](.*?)[.]crc$".r
    def isFileNameOfCrcFile(filename: String) = {
      val m = CrcPattern.findFirstMatchIn(filename)
      !m.isEmpty
    }
  }
}

