package com.twitter.scalding.spark_backend

import org.scalatest.{ BeforeAndAfter, FunSuite, PropSpec }
import org.apache.hadoop.io.IntWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ Config, Execution, TextLine, WritableSequenceFile }
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.memory_backend.MemoryMode
import java.io.File
import java.nio.file.Paths

import SparkMode.SparkConfigMethods
import com.twitter.scalding.spark_backend.SparkPlanner.ConfigPartitionComputer
import org.scalatest.prop.PropertyChecks

class SparkBackendTests extends FunSuite with BeforeAndAfter {

  private def removeDir(path: String): Unit = {
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        sys.error(s"Unable to delete ${file.getAbsolutePath}")
    }

    deleteRecursively(new File(path))
  }

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

  def sparkMatchesIterable[A: Ordering](t: Execution[Iterable[A]], iter: Iterable[A], conf: Config = Config.empty) = {
    val smode = SparkMode.default(session)
    val semit = t.waitFor(conf, smode).get

    assert(semit.toList.sorted == iter.toList.sorted)
  }

  def sparkMatchesMemory[A: Ordering](t: TypedPipe[A]) =
    sparkMatchesIterable(t.toIterableExecution,
      t.toIterableExecution.waitFor(Config.empty, MemoryMode.empty).get)

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

    sparkMatchesMemory {
      val input = TypedPipe.from(0 to 1000)
      // many merges
      Monoid.sum((2 to 8).map { i => input.filter(_ % i == 0) })
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

  def tmpPath(suffix: String): String =
    Paths.get(System.getProperty("java.io.tmpdir"),
      "scalding",
      "spark_backend",
      suffix).toString

  test("writeExecution works with TextLine") {
    val path = tmpPath("textline")
    sparkMatchesIterable({
      val loc = TextLine(path)
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2)
        .sorted
        .foldLeft(0)(_ - _)
        .toTypedPipe
        .map(_.toString)
        .writeExecution(loc)
        .flatMap { _ =>
          TypedPipe.from(loc).toIterableExecution
        }

    }, (0 to 100000).groupBy(_ % 2).mapValues(_.foldLeft(0)(_ - _)).map(_.toString))

    removeDir(path)
  }

  test("writeExecution works with IntWritable") {
    val path = tmpPath("int_writable")
    sparkMatchesIterable({
      val loc = WritableSequenceFile[IntWritable, IntWritable](path)
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2)
        .sorted
        .foldLeft(0)(_ - _)
        .toTypedPipe
        .map { case (k, v) => (new IntWritable(k), new IntWritable(v)) }
        .writeExecution(loc)
        .flatMap { _ =>
          TypedPipe.from(loc)
            .map { case (k, v) => (k.get, v.get) }
            .toIterableExecution
        }

    }, (0 to 100000).groupBy(_ % 2).mapValues(_.foldLeft(0)(_ - _)))

    removeDir(path)
  }

  test("forceToDisk works") {
    sparkMatchesIterable({
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2)
        .sorted
        .foldLeft(0)(_ - _)
        .toTypedPipe
        .map(_.toString)
        .forceToDiskExecution
        .flatMap(_.toIterableExecution)

    }, (0 to 100000).groupBy(_ % 2).mapValues(_.foldLeft(0)(_ - _)).map(_.toString))
  }

  test("forceToDisk works with no persistance") {
    sparkMatchesIterable({
      val input = TypedPipe.from(0 to 100000)
      input.groupBy(_ % 2)
        .sorted
        .foldLeft(0)(_ - _)
        .toTypedPipe
        .map(_.toString)
        .forceToDisk
        .toIterableExecution

    }, (0 to 100000).groupBy(_ % 2).mapValues(_.foldLeft(0)(_ - _)).map(_.toString),
      Config.empty.setForceToDiskPersistMode("NONE"))
  }
}

class ConfigPartitionComputerTest extends PropSpec with PropertyChecks {
  property("when no config or number of reducers are given, returns the current number of partitions (or 1)") {
    val pc = ConfigPartitionComputer(Config.empty, None)
    forAll { i: Int =>
      if (i >= 1) assert(pc(i) == i)
      else if (i > 0) assert(pc(i) == 1)
    }
  }

  property("when number of reducers are given but no scaling factor, returns the number of reducers") {
    val pc = ConfigPartitionComputer(Config.empty, Some(10))
    forAll { i: Int =>
      if (i >= 0) assert(pc(i) == 10)
    }
  }

  property("when reducer scaling factor given, scales the number of reducers") {
    val pc = ConfigPartitionComputer(Config.empty.setReducerScaling(2.0), Some(10))
    forAll { i: Int =>
      if (i > 0) assert(pc(i) == 20)
    }
  }

  property("when max partition count given, caps the result") {
    val pc = ConfigPartitionComputer(Config.empty.setMaxPartitionCount(10), None)
    forAll { i: Int =>
      if (i > 0) assert(pc(i) == Math.min(10, i))
    }
  }
}
