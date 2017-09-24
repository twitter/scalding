package com.twitter.scalding.estimation.memory

import org.apache.hadoop.mapred.JobConf
import org.scalatest.{ Matchers, WordSpec }

class MemoryEstimatorStepStrategyTest extends WordSpec with Matchers {
  "A Memory estimator step strategy" should {
    "set xmx settings correctly" in {
      val conf = confWith("test.opts", "-Xmx3500m -Djava.net.preferIPv4Stack=true -Xms34m")

      MemoryEstimatorStepStrategy.setXmxMemory("test.opts", 1024, conf)

      conf.get("test.opts") shouldBe "-Djava.net.preferIPv4Stack=true -Xmx1024m"
    }

    "set xmx settings correctly with empty original config" in {
      val conf = confWith(Map.empty)

      MemoryEstimatorStepStrategy.setXmxMemory("test.opts", 1024, conf)

      conf.get("test.opts") shouldBe " -Xmx1024m"
    }
  }

  def confWith(key: String, value: String): JobConf =
    confWith(Map(key -> value))

  def confWith(values: Map[String, String]): JobConf = {
    val conf = new JobConf(false)

    values.foreach {
      case (k, v) =>
        conf.set(k, v)
    }

    conf
  }
}
