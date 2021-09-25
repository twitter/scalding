package com.twitter.scalding.platform

import com.twitter.scalding.{ Config, Execution, TypedPipe, TypedTsv }
import org.scalatest.{ Matchers, WordSpec }
import scala.io.Source

object InAndOutExecution extends Function[Config, Execution[Unit]] {
  override def apply(config: Config): Execution[Unit] =
    TypedPipe
      .from(TypedTsv[String]("input"))
      .writeExecution(TypedTsv[String]("output"))
}

object OneDistributedCacheExecution extends Function[Config, Execution[Unit]] {
  val one: (String, Seq[String]) = ("one", Seq("a", "d"))
  val input = Seq("a", "b", "c", "d")
  val output = Seq("a", "d")

  override def apply(v1: Config): Execution[Unit] =
    Execution.withCachedFile("one") { theOne =>
      lazy val symbols = Source
        .fromFile(theOne.file)
        .getLines()
        .toSeq

      TypedPipe
        .from(TypedTsv[String]("input"))
        .filter { symbol =>
          symbols.contains(symbol)
        }
        .writeExecution(TypedTsv[String]("output"))
    }
}

object MultipleDistributedCacheExecution extends Function[Config, Execution[Unit]] {
  val first: (String, Seq[String]) = ("first", Seq("a", "d"))
  val second: (String, Seq[String]) = ("second", Seq("c"))
  val input = Seq("a", "b", "c", "d")
  val output = Seq("a", "c", "d")

  override def apply(v1: Config): Execution[Unit] =
    Execution.withCachedFile("first") { theFirst =>
      Execution.withCachedFile("second") { theSecond =>
        lazy val firstSymbols =
          Source
            .fromFile(theFirst.file)
            .getLines()
            .toSeq

        lazy val secondSymbols =
          Source
            .fromFile(theSecond.file)
            .getLines()
            .toSeq

        lazy val symbols = firstSymbols ++ secondSymbols

        TypedPipe
          .from(TypedTsv[String]("input"))
          .filter { symbol =>
            symbols.contains(symbol)
          }
          .writeExecution(TypedTsv[String]("output"))
      }
    }
}

class PlatformExecutionTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  "An InAndOutTest" should {
    val inAndOut = Seq("a", "b", "c")

    "reading then writing shouldn't change the data" in {
      HadoopPlatformExecutionTest(InAndOutExecution, cluster)
        .source("input", inAndOut)
        .sink[String]("output") { _.toSet shouldBe inAndOut.toSet }
        .run()
    }
  }

  "An DistributedCacheTest" should {
    "have access to file on hadoop" in {
      import OneDistributedCacheExecution._

      HadoopPlatformExecutionTest(OneDistributedCacheExecution, cluster)
        .data(one)
        .source("input", input)
        .sink[String]("output") { _ shouldBe output }
        .run()
    }

    "have access to multiple files on hadoop" in {
      import MultipleDistributedCacheExecution._

      HadoopPlatformExecutionTest(MultipleDistributedCacheExecution, cluster)
        .data(first)
        .data(second)
        .source("input", input)
        .sink[String]("output") { _ shouldBe output }
        .run()
    }
  }
}
