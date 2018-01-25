package com.twitter.scalding.platform

import com.twitter.scalding._
import com.twitter.scalding.source.TypedText

import java.io.{ BufferedWriter, File, FileWriter }

import org.slf4j.LoggerFactory

import scala.util.Try

trait HadoopPlatform[P, R, T <: HadoopPlatform[P, R, T]] {
  private val LOG = LoggerFactory.getLogger(getClass)

  val cons: (P) => R
  val cluster: LocalCluster

  val dataToCreate: Seq[(String, Seq[String])]
  val sourceWriters: Seq[P => R]
  val sourceReaders: Seq[Mode => Unit]

  def arg(key: String, value: String): T

  def data(data: (String, Seq[String])): T

  def source[K: TypeDescriptor](location: String, data: Seq[K]): T =
    source(TypedText.tsv[K](location), data)

  def source[K](out: TypedSink[K], data: Seq[K]): T

  def sink[K: TypeDescriptor](location: String)(toExpect: Seq[K] => Unit): T =
    sink(TypedText.tsv[K](location))(toExpect)

  def sink[K](in: Mappable[K])(toExpect: Seq[K] => Unit): T

  def run(): Unit

  def runExpectFailure[K](fn: Throwable => K): K =
    fn(Try { run() }.failed.get)

  def init(cons: P => R): R

  def execute(unit: R): Unit

  protected def createSources(): Unit = {
    dataToCreate foreach {
      case (location, lines) =>
        val tmpFile = File.createTempFile("hadoop_platform", "job_test")
        tmpFile.deleteOnExit()
        if (lines.nonEmpty) {
          val os = new BufferedWriter(new FileWriter(tmpFile))
          os.write(lines.head)
          lines.tail.foreach { str =>
            os.newLine()
            os.write(str)
          }
          os.close()
        }
        cluster.putFile(tmpFile, location)
        tmpFile.delete()
    }

    sourceWriters.foreach { cons => execute(init(cons)) }
  }

  protected def checkSinks(): Unit = {
    LOG.debug("Executing sinks")
    sourceReaders.foreach { _(cluster.mode) }
  }
}