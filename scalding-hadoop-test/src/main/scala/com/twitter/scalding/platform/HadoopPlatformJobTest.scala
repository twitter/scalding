/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding.platform

import com.twitter.scalding._

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.Buffer

import org.slf4j.LoggerFactory

object HadoopPlatformJobTest {
  def apply(cons : (Args) => Job, cluster: LocalCluster) = {
    new HadoopPlatformJobTest(cons, cluster)
  }
}

/**
 * This class is used to construct unit tests in scalding which
 * use Hadoop's MiniCluster to more fully simulate and test
 * the logic which is deployed in a job.
 */
class HadoopPlatformJobTest(cons : (Args) => Job, cluster: LocalCluster) {
  private val LOG = LoggerFactory.getLogger(getClass)

  private var argsMap = Map[String, List[String]]()
  private val dataToCreate = Buffer[(String, Seq[String])](("dummyInput", Seq("dummyLine")))
  private val sourceWriters = Buffer[Args => Job]()
  private val sourceReaders = Buffer[Mode => Unit]()

  cluster.addClassSourceToClassPath(cons.getClass)

  def arg(inArg: String, value: List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg: String, value: String) = {
    argsMap += inArg -> List(value)
    this
  }

  def source[T: Manifest](location: String, data: Seq[T]): this.type = source(TypedTsv[T](location), data)

  def source[T](out: TypedSink[T], data: Seq[T]): this.type = {
    sourceWriters.+=({ args: Args =>
      new Job(args) {
        TypedPipe.from(TypedTsv[String]("dummyInput")).flatMap { _ => data }.write(out)
      }
    })
    this
  }

  def sink[T: Manifest](location: String)(toExpect: Seq[T] => Unit): this.type = sink(TypedTsv[T](location))(toExpect)

  def sink[T](in: Mappable[T])(toExpect: Seq[T] => Unit): this.type = {
    sourceReaders.+=(mode => toExpect(in.toIterator(mode).toSeq))
    this
  }

  private def createSources() {
    dataToCreate foreach { case (location, lines) =>
      val tmpFile = File.createTempFile("hadoop_platform", "job_test")
      tmpFile.deleteOnExit()
      if (!lines.isEmpty) {
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

    sourceWriters.foreach { cons => runJob(initJob(cons)) }
  }

  private def checkSinks() {
    LOG.debug("Executing sinks")
    sourceReaders.foreach { _(cluster.mode) }
  }

  def run {
    createSources()
    runJob(initJob(cons))
    checkSinks()
  }

  private def initJob(cons: Args => Job): Job = cons(Mode.putMode(cluster.mode, new Args(argsMap)))

  @annotation.tailrec
  private final def runJob(job: Job) {
    job.run
    job.clear
    job.next match {
      case Some(nextJob) => runJob(nextJob)
      case None => ()
    }
  }
}
