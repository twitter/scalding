package com.twitter.scalding.spark_backend

import org.scalatest.{ FunSuite, BeforeAndAfter }
import org.apache.spark.{ SparkContext, SparkConf }
import com.twitter.scalding.Config
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.memory_backend.MemoryMode
import scala.concurrent.ExecutionContext

class SparkBackendTests extends FunSuite with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "spark-backent-tests"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
    sc = null
  }

  def sparkMatchesMemory[A: Ordering](t: TypedPipe[A]) = {
    val memit = t.toIterableExecution.waitFor(Config.empty, MemoryMode.empty).get

    val rdd = SparkPlanner.R.toRDD(SparkPlanner.plan(sc, Config.empty)(ExecutionContext.global)(t))

    assert(rdd.toLocalIterator.toList.sorted == memit.toList.sorted)
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
}
