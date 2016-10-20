package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

class ModeParseHadoopSpec extends WordSpec with Matchers {
  "Parsing for Hadoop" should {
    val parser = new ExecutionApp {
      def job = Execution.from(())
    }
    "parse args correctly (local)" in {
      val (conf, mode) = parser.config(Array("--local"))
      mode shouldBe a[LocalMode]

      mode.name === "local"
    }

    "parse args correctly (hdfs, LEGACY)" in {
      val (conf, mode) = parser.config(Array("--hdfs"))
      mode shouldBe a[LegacyHadoopMode]

      mode.name === "hadoop"
    }

    "parse args correctly (hadoop1)" in {
      val (conf, mode) = parser.config(Array("--hadoop1"))
      mode shouldBe a[LegacyHadoopMode]

      mode.name === "hadoop"
    }

    "parse args correctly (autoCluster)" in {
      val (conf, mode) = parser.config(Array("--autoCluster"))
      mode shouldBe a[LegacyHadoopMode]

      mode.name === "hadoop"
    }
  }
}