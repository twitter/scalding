package com.twitter.scalding

import java.io.File

/**
 * Miscellaneous bits of code related to checking whether the expected
 * output files are within a directory, etc.
 */
object TestFileUtil {
  import scala.language.implicitConversions

  case class RichDirectory(dir: File) {
    lazy val allFiles = dir.listFiles

    lazy val fileNameSet = allFiles.map(_.getName).toSet

    def fileNameSetExSuccess = fileNameSet.filterNot(_ == "_SUCCESS")

    def partFiles = fileNameSet.filter(_.startsWith("part-"))

    def list = dir.list
    def listFiles = allFiles

    implicit def backToFile = dir
    override def toString = dir.toString
  }
  implicit def toRichDirectory(dir: File) = RichDirectory(dir)

  object RichDirectory {
    def apply(dirname: String): RichDirectory = RichDirectory(new File(dirname))
    def apply(parent: File, dirname: String): RichDirectory = RichDirectory(new File(parent, dirname))
    def apply(parent: RichDirectory, dirname: String): RichDirectory = RichDirectory(new File(parent.dir, dirname))
  }
}

