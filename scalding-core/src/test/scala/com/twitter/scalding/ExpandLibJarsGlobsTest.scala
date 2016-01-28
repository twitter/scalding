package com.twitter.scalding

import java.io.File
import java.nio._
import java.nio.file._
import org.scalatest.{ Matchers, WordSpec }

class ExpandLibJarsGlobsTest extends WordSpec with Matchers {
  def touch(parent: File, p: String): String = {
    val f = new File(parent, p)
    f.createNewFile
    f.getAbsolutePath
  }

  "ExpandLibJarsGlobs" should {
    "expand entries" in {
      val tmpRoot = new File(System.getProperty("java.io.tmpdir"), System.currentTimeMillis.toString)
      require(tmpRoot.mkdirs(), "Failed to make temporary directory")
      tmpRoot.deleteOnExit()

      // Has a side effect, but returns us the jars absolute paths
      val jars = (0 until 20).map { idx =>
        touch(tmpRoot, s"myF_${idx}.jar")
      }

      val resultingLibJars1 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/*.jar"))(1).split(",")
      assert(resultingLibJars1.sorted.toList == jars.sorted.toList)

      val resultingLibJars2 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/"))(1).split(",")
      assert(resultingLibJars2.sorted.toList == jars.sorted.toList)

      val resultingLibJars3 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/*"))(1).split(",")
      assert(resultingLibJars3.sorted.toList == jars.sorted.toList)
    }

    "Skips over unmatched entries" in {
      val tmpRoot = new File(System.getProperty("java.io.tmpdir"), System.currentTimeMillis.toString)
      require(tmpRoot.mkdirs(), "Failed to make temporary directory")
      tmpRoot.deleteOnExit()

      // Has a side effect, but returns us the jars absolute paths
      val jars = (0 until 20).map { idx =>
        touch(tmpRoot, s"myF_${idx}.jar")
      }

      val resultingLibJars1 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/*.zip"))(1).split(",").filter(_.nonEmpty)
      assert(resultingLibJars1.isEmpty)
    }

  }
}
