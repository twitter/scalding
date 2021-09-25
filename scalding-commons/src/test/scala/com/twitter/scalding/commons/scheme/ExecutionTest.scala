package com.twitter.scalding.commons.scheme

import com.twitter.scalding.source.TypedSequenceFile
import com.twitter.scalding.{
  Config,
  Execution,
  Hdfs,
  Local,
  TypedPipe
}
import org.apache.hadoop.conf.Configuration
import org.scalatest.{ Matchers, WordSpec }
import scala.util.{ Failure, Success }

class ExecutionTest extends WordSpec with Matchers {
  object TestPath {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    def prefix = getCurrentDirectory.split("/").last match {
      case "scalding-commons" => getCurrentDirectory
      case _ => getCurrentDirectory + "/scalding-commons"
    }
    val testfsPathRoot = prefix + "/src/test/resources/com/twitter/scalding/test_filesystem/"
  }

  implicit class ExecutionTestHelper[T](ex: Execution[T]) {
    def shouldSucceed(): T = {
      val r = ex.waitFor(Config.default, Local(true))
      r match {
        case Success(s) => s
        case Failure(e) => fail(s"Failed running execution, exception:\n$e")
      }
    }

    def shouldSucceedHadoop(): T = {
      val mode = Hdfs(true, new Configuration)
      val r = ex.waitFor(Config.defaultFrom(mode), mode)
      r match {
        case Success(s) => s
        case Failure(e) => fail(s"Failed running execution, exception:\n$e")
      }
    }

    def shouldFail(): Unit = {
      val r = ex.waitFor(Config.default, Local(true))
      assert(r.isFailure)
    }

    def shouldFailWith(message: String): Unit = {
      val r = ex.waitFor(Config.default, Local(true))
      assert(r.isFailure)
      r.failed.get.getMessage shouldBe message
    }
  }

  "Execution" should {
    class TypedSequenceFileSource[T](override val path: String) extends TypedSequenceFile[T](path) with CombinedSequenceFileScheme

    "toIterableExecution works correctly on partly empty input (empty part, part with value)" in {
      val exec =
        TypedPipe
          .from(new TypedSequenceFileSource[(Long, Long)](TestPath.testfsPathRoot + "test_data/2013/09"))
          .toIterableExecution
          .map { _.toSet }

      val res = exec.shouldSucceedHadoop()

      assert(res == Set((1L, 1L)))
    }

    "toIterableExecution works correctly on partly empty input (empty part, part with value, empty part)" in {
      val exec =
        TypedPipe
          .from(new TypedSequenceFileSource[(Long, Long)](TestPath.testfsPathRoot + "test_data/2013/10"))
          .toIterableExecution
          .map { _.toSet }

      val res = exec.shouldSucceedHadoop()

      assert(res == Set((1L, 1L)))
    }
  }
}
