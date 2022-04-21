package com.twitter.scalding.spark_backend

import com.twitter.scalding.{Config, Execution}
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.typed.memory_backend.MemoryMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.Paths

class SparkBaseTest extends FunSuite with BeforeAndAfter {

  def removeDir(path: String): Unit = {
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        sys.error(s"Unable to delete ${file.getAbsolutePath}")
    }

    deleteRecursively(new File(path))
  }

  val master = "local[2]"
  val appName = "spark-backend-tests"

  var session: SparkSession = _

  before {
    val conf =
      new SparkConf()
        .setMaster(master)
        .setAppName(appName)
        .set(
          "spark.driver.host",
          "localhost"
        ) // this is needed to work on OSX when disconnected from the network

    session = SparkSession.builder.config(conf).getOrCreate()
  }

  after {
    session.stop()
    session = null
  }

  def sparkMatchesIterable[A: Ordering](
      t: Execution[Iterable[A]],
      iter: Iterable[A],
      conf: Config = Config.empty
  ) = {
    val smode = SparkMode.default(session)
    val semit = t.waitFor(conf, smode).get

    assert(semit.toList.sorted == iter.toList.sorted)
  }

  def sparkMatchesMemory[A: Ordering](t: TypedPipe[A]) =
    sparkMatchesIterable(
      t.toIterableExecution,
      t.toIterableExecution.waitFor(Config.empty, MemoryMode.empty).get
    )

  def sparkRetrieveCounters[A](t: TypedPipe[A]) = {
    val smode = SparkMode.default(session)
    val (eiter, ecounters) = t.toIterableExecution.getCounters.waitFor(Config.empty, smode).get
    ecounters
  }

  def sparkRetrieveCounters(t: Execution[Any], conf: Config = Config.empty) = {
    val smode = SparkMode.default(session)
    val (eiter, ecounters) = t.getCounters.waitFor(conf, smode).get
    ecounters
  }

  def tmpPath(suffix: String): String =
    Paths.get(System.getProperty("java.io.tmpdir"), "scalding", "spark_backend", suffix).toString

}
