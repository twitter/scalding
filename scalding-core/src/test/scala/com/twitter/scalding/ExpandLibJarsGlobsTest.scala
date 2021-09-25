package com.twitter.scalding

import java.io.File
import org.scalatest.{ Matchers, WordSpec }

class ExpandLibJarsGlobsTest extends WordSpec with Matchers {
  def touch(parent: File, p: String): String = {
    val f = new File(parent, p)
    f.createNewFile
    f.getAbsolutePath
  }

  def getTmpRoot = {
    val tmpRoot = new File(System.getProperty("java.io.tmpdir"), scala.util.Random.nextInt(Int.MaxValue).toString)
    require(tmpRoot.mkdirs(), "Failed to make temporary directory")
    tmpRoot.deleteOnExit()
    tmpRoot
  }

  "ExpandLibJarsGlobs" should {
    "expand entries" in {

      val tmpRoot = getTmpRoot
      // Has a side effect, but returns us the jars absolute paths
      val jars = (0 until 20).map { idx =>
        touch(tmpRoot, s"myF_${idx}.jar")
      } ++ (0 until 20).map { idx =>
        touch(tmpRoot, s".myHidden.jar.myF_${idx}.jar")
      }

      val resultingLibJars1 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/*.jar"))(1).split(",")
      assert(resultingLibJars1.sorted.toList == jars.sorted.toList)

      val resultingLibJars2 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/"))(1).split(",")
      assert(resultingLibJars2.sorted.toList == jars.sorted.toList)

      val resultingLibJars3 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/*"))(1).split(",")
      assert(resultingLibJars3.sorted.toList == jars.sorted.toList)
    }

    "Skips over unmatched entries" in {
      val tmpRoot = getTmpRoot

      // Has a side effect, but returns us the jars absolute paths
      val jars = (0 until 20).map { idx =>
        touch(tmpRoot, s"myF_${idx}.jar")
      } ++ (0 until 20).map { idx =>
        touch(tmpRoot, s".myHidden.jar.myF_${idx}.jar")
      }

      val resultingLibJars1 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot.getAbsolutePath}/*.zip"))(1).split(",").filter(_.nonEmpty)
      assert(resultingLibJars1.isEmpty)
    }

    "Multiple paths in libjars" in {
      val tmpRoot1 = getTmpRoot
      val tmpRoot2 = getTmpRoot

      // Has a side effect, but returns us the jars absolute paths
      val jars1 = (0 until 20).map { idx =>
        touch(tmpRoot1, s"myF_${idx}.jar")
      } ++ (0 until 20).map { idx =>
        touch(tmpRoot1, s".myHidden.jar.myF_${idx}.jar")
      }

      val jars2 = (0 until 1).map { idx =>
        touch(tmpRoot2, s"myF_${idx}.jar")
      }

      // Using wildcards for both
      val resultingLibJars1 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot1.getAbsolutePath}/*.jar,${tmpRoot2.getAbsolutePath}/*.jar"))(1).split(",")
      assert(resultingLibJars1.sorted.toList == (jars1 ++ jars2).sorted.toList)

      // No wildcards for second dir
      val resultingLibJars2 = ExpandLibJarsGlobs(Array("-libjars", s"${tmpRoot1.getAbsolutePath}/*.jar,${tmpRoot2.getAbsolutePath}/myF_0.jar"))(1).split(",")
      assert(resultingLibJars2.sorted.toList == (jars1 ++ jars2).sorted.toList)

    }

  }
}
