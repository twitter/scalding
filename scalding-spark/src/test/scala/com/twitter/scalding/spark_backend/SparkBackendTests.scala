package com.twitter.scalding.spark_backend

import org.scalatest.{ FunSuite, BeforeAndAfter }
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.twitter.scalding.Config
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.memory_backend.MemoryMode
import scala.concurrent.{ Await, ExecutionContext }

class SparkBackendTests extends FunSuite with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "spark-backent-tests"

  private var session: SparkSession = _

  before {
    val conf =
      new SparkConf()
        .setMaster(master)
        .setAppName(appName)
        .set("spark.driver.host", "localhost") // this is needed to work on OSX when disconnected from the network

    session = SparkSession.builder.config(conf).getOrCreate()
  }

  after {
    session.stop()
    session = null
  }

  def sparkMatchesMemory[A: Ordering](t: TypedPipe[A]) = {
    val memit = t.toIterableExecution.waitFor(Config.empty, MemoryMode.empty).get

    val semit = t.toIterableExecution.waitFor(Config.empty, SparkMode.empty(session)).get

    assert(semit.toList.sorted == memit.toList.sorted)
  }

  test("some basic map-only operations work") {
    sparkMatchesMemory(TypedPipe.from(0 to 100))
    sparkMatchesMemory(TypedPipe.from(0 to 100).map(_ * 2))
    sparkMatchesMemory(TypedPipe.from(0 to 100).map { x => (x, x * Int.MaxValue) })

    sparkMatchesMemory(TypedPipe.from(0 to 100)
      .map { x => (x, x * Int.MaxValue) }
      .filter { case (k, v) => k > v })
  }

  test("test with map-only with merge") {
    sparkMatchesMemory {
      val input = TypedPipe.from(0 to 1000)
      val (evens, odds) = input.partition(_ % 2 == 0)

      evens ++ odds
    }
  }

  test("sumByLocalKeys matches") {
    sparkMatchesMemory {
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2).sumByLocalKeys
    }
  }

  test(".group.foldLeft works") {
    sparkMatchesMemory {
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2).foldLeft(0)(_ + _)
    }
  }

  test(".group.sorted works") {
    sparkMatchesMemory {
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2).sorted.toTypedPipe
    }
  }
  test(".group.sorted.foldLeft works") {
    sparkMatchesMemory {
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2).sorted.foldLeft(0)(_ - _).toTypedPipe
    }
  }

  test("joins work") {
    sparkMatchesMemory {
      val inputLeft = TypedPipe.from(0 to 100000 by 3)
      val inputRight = TypedPipe.from(1 to 100000 by 3)
      inputLeft.groupBy(_ / 10).join(inputRight.groupBy(_ / 3)).sum.toTypedPipe
    }
  }

  test("hashJoin works") {
    sparkMatchesMemory {
      val inputLeft = TypedPipe.from(0 to 100000 by 3)
      val inputRight = TypedPipe.from(1 to 1000 by 3)
      inputLeft.groupBy(_ / 10).hashJoin(inputRight.groupBy(_ / 3))
    }
  }

  test("crossValue works") {
    sparkMatchesMemory {
      val inputLeft = TypedPipe.from(0 to 100000 by 3)
      inputLeft.cross(ValuePipe("wee"))
    }
  }
}
