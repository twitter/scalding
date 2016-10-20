package com.twitter.scalding

import org.scalatest.{Matchers, WordSpec}

class ModeParseFlinkSpec extends WordSpec with Matchers {
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

    "parse args correctly (flink)" in {
      val (conf, mode) = parser.config(Array("--flink"))
      mode shouldBe a[FlinkMode]

      mode.name === "flink"
    }

    "parse args correctly (autoCluster)" in {
      val (conf, mode) = parser.config(Array("--autoCluster"))
      mode shouldBe a[FlinkMode]

      mode.name === "flink"
    }
  }
}