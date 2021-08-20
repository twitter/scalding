package com.twitter.scalding.beam_backend

import com.twitter.scalding.{Config, TextLine, TypedPipe}
import java.io.File
import java.nio.file.Paths
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.io.Source

class BeamBackendTests extends FunSuite with BeforeAndAfter {

  private var pipelineOptions: PipelineOptions = _
  private var testPath: String = _

  def tmpPath(suffix: String): String = {
    Paths.get(testPath, suffix).toString
  }

  before {
    testPath = Paths.get(
      System.getProperty("java.io.tmpdir"),
      "scalding",
      "beam_backend").toString
    pipelineOptions = PipelineOptionsFactory.create()
  }

  after {
    removeDir(testPath)
  }

  test("map & flatMap operations"){
    val outRoute = tmpPath("out1")
    val ex = TypedPipe
      .from(0 to 10)
      .map(x => x * 2)
      .flatMap(x => 0 to x)
      .map(x => (x, x))
      .mapValues(x => x * 2)
      .flatMapValues(x => 0 to x)
      .map(_.toString)
      .writeExecution(TextLine(outRoute))

    val bmode = BeamMode.default(pipelineOptions)
    ex.waitFor(Config.empty, bmode).get
    val expectedResult = List
      .range(0, 11)
      .map(x => x * 2)
      .flatMap(x => 0 to x)
      .map(x => (x, x))
      .map(x => (x._1, x._2 * 2))
      .flatMap(x => (0 to x._2).map(nx => (x._1, nx)))
      .map(_.toString)
      .sorted
    val result = getContents(testPath, outRoute).sorted

    assert(result == expectedResult)

    removeDir(testPath)
  }

  test("filter operations"){
    val outRoute = tmpPath("out1")
    val ex = TypedPipe
      .from(0 to 100)
      .filter(x => x % 2 == 0)
      .map(x => (x, x))
      .filterKeys(x => x % 4 == 0)
      .map(_.toString)
      .writeExecution(TextLine(outRoute))

    val bmode = BeamMode.default(pipelineOptions)
    ex.waitFor(Config.empty, bmode).get
    val expectedResult = List
      .range(0, 101)
      .filter(x => x % 2 == 0)
      .map(x => (x, x))
      .filter(x => x._1 % 4 == 0)
      .map(_.toString)
      .sorted
    val result = getContents(testPath, outRoute).sorted

    assert(result == expectedResult)

    removeDir(testPath)
  }

  private def getContents(path: String, prefix: String): List[String] = {
    new File(path).listFiles.flatMap(file => {
      if(file.getPath.startsWith(prefix)){
        Source.fromFile(file).getLines().flatMap(line => line.split("\\s+").toList)
      }else List.empty[String]
    }).toList
  }

  private def removeDir(path: String): Unit = {
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        sys.error(s"Unable to delete ${file.getAbsolutePath}")
    }
    deleteRecursively(new File(path))
  }
}
