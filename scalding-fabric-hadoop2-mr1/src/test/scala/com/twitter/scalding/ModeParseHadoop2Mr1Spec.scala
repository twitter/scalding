package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

class ModeParseHadoop2Mr1Spec extends WordSpec with Matchers {
  "Parsing for Hadoop" should {
    val parser = new ExecutionApp {
      def job = Execution.from(())
    }
    "parse args correctly (local)" in {
      val (conf, mode) = parser.config(Array("--local"))
      mode shouldBe a[LocalMode]

      mode.name === "local"
    }

    "reject legacy args (hdfs)" in {
      the[Exception] thrownBy {
        val (conf, mode) = parser.config(Array("--hdfs"))
      } shouldBe a[ArgsException]
    }

    "parse args correctly (hadoop2-mr1)" in {
      val (conf, mode) = parser.config(Array("--hadoop2-mr1"))
      mode shouldBe a[Hadoop2Mr1Mode]

      mode.name === "hadoop2-mr1"
    }

    "parse args correctly (autoCluster)" in {
      val (conf, mode) = parser.config(Array("--autoCluster"))
      mode shouldBe a[Hadoop2Mr1Mode]

      mode.name === "hadoop2-mr1"
    }
  }
}