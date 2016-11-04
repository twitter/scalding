package com.twitter.scalding

import com.twitter.algebird.monad.Reader

import com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MacroEqualityOrderedSerialization
import com.twitter.scalding.serialization.OrderedSerialization

import java.nio.file.FileSystems
import java.nio.file.Path

import org.scalatest.{ Matchers, WordSpec }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.util.Random
import scala.util.{ Try, Success, Failure }

import ExecutionContext._

class ExecutionHadoopExtraTest extends ExecutionTest /* WordSpec with Matchers */ {
  "ExecutionApp (continued)" should {
    val parser = new ExecutionApp {
      def job = Execution.from(())
    }

    "parse args correctly (legacy hdfs)" in {
      val (conf1, mode) = parser.config(Array("--test", "-Dmapred.reduce.tasks=110", "--hdfs"))

      mode shouldBe a[LegacyHadoopMode]
      val hconf = mode.asInstanceOf[LegacyHadoopMode].jobConf

      conf1.get("mapred.reduce.tasks") should contain("110")
      conf1.getArgs.boolean("test") shouldBe true
      hconf.get("mapred.reduce.tasks") shouldBe "110"
    }
    "parse args correctly (hadoop1) " in {
      val (conf1, mode) = parser.config(Array("--test", "-Dmapred.reduce.tasks=110", "--hadoop1"))

      mode shouldBe a[LegacyHadoopMode]
      val hconf = mode.asInstanceOf[LegacyHadoopMode].jobConf

      conf1.get("mapred.reduce.tasks") should contain("110")
      conf1.getArgs.boolean("test") shouldBe true
      hconf.get("mapred.reduce.tasks") shouldBe "110"
    }

    "parse args correctly (autoCluster) " in {
      val (conf1, mode) = parser.config(Array("--test", "--autoCluster"))

      mode shouldBe a[ClusterMode] // Can be anything that's available in the classpath.
    }

  }
}
