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

import scala.collection.JavaConverters._
import cascading.flow.Flow
import com.twitter.scalding._
import com.twitter.scalding.source.TypedText

import java.io.{ BufferedWriter, File, FileWriter }

import org.apache.hadoop.mapred.JobConf

import org.slf4j.LoggerFactory

/**
 * This class is used to construct unit tests in scalding which
 * use Hadoop's MiniCluster to more fully simulate and test
 * the logic which is deployed in a job.
 */
case class HadoopPlatformJobTest(
  cons: (Args) => Job,
  cluster: LocalCluster,
  argsMap: Map[String, List[String]] = Map.empty,
  dataToCreate: Seq[(String, Seq[String])] = Vector(),
  sourceWriters: Seq[Args => Job] = Vector.empty,
  sourceReaders: Seq[Mode => Unit] = Vector.empty,
  flowCheckers: Seq[Flow[_] => Unit] = Vector.empty) {
  private val LOG = LoggerFactory.getLogger(getClass)

  def arg(inArg: String, value: List[String]): HadoopPlatformJobTest = copy(argsMap = argsMap + (inArg -> value))

  def arg(inArg: String, value: String): HadoopPlatformJobTest = arg(inArg, List(value))

  def source[T: TypeDescriptor](location: String, data: Seq[T]): HadoopPlatformJobTest = source(TypedText.tsv[T](location), data)

  def source[T](out: TypedSink[T], data: Seq[T]): HadoopPlatformJobTest =
    copy(sourceWriters = sourceWriters :+ { args: Args =>
      new Job(args) {
        TypedPipe.from(List("")).flatMap { _ => data }.write(out)
      }
    })

  def sink[T: TypeDescriptor](location: String)(toExpect: Seq[T] => Unit): HadoopPlatformJobTest =
    sink(TypedText.tsv[T](location))(toExpect)

  def sink[T](in: Mappable[T])(toExpect: Seq[T] => Unit): HadoopPlatformJobTest =
    copy(sourceReaders = sourceReaders :+ { m: Mode => toExpect(in.toIterator(Config.defaultFrom(m), m).toSeq) })

  def inspectCompletedFlow(checker: Flow[_] => Unit): HadoopPlatformJobTest =
    copy(flowCheckers = flowCheckers :+ checker)

  private def createSources() {
    dataToCreate foreach {
      case (location, lines) =>
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
    System.setProperty("cascading.update.skip", "true")
    val job = initJob(cons)
    cluster.addClassSourceToClassPath(cons.getClass)
    cluster.addClassSourceToClassPath(job.getClass)
    createSources()
    runJob(job)
    checkSinks()
    flowCheckers.foreach { checker =>
      job.completedFlow.collect {
        case f: Flow[_] => checker(f)
      }
    }
  }

  private def initJob(cons: Args => Job): Job = cons(Mode.putMode(cluster.mode, new Args(argsMap)))

  @annotation.tailrec
  private final def runJob(job: Job) {
    // create cascading 3.0 planner trace files during tests
    if (System.getenv.asScala.getOrElse("SCALDING_CASCADING3_DEBUG", "0") == "1") {
      System.setProperty("cascading.planner.plan.path", "target/test/cascading/traceplan/" + job.name)
      System.setProperty("cascading.planner.plan.transforms.path", "target/test/cascading/traceplan/" + job.name + "/transform")
      System.setProperty("cascading.planner.stats.path", "target/test/cascading/traceplan/" + job.name + "/stats")
    }

    job.run
    job.clear
    job.next match {
      case Some(nextJob) => runJob(nextJob)
      case None => ()
    }
  }
}
