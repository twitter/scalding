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

import cascading.flow.Flow
import com.twitter.scalding._

import org.apache.hadoop.mapred.JobConf

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
  flowCheckers: Seq[Flow[JobConf] => Unit] = Vector.empty) extends HadoopPlatform[Args, Job, HadoopPlatformJobTest] {

  override def arg(key: String, value: String): HadoopPlatformJobTest =
    copy(argsMap = argsMap + (key -> List(value)))

  override def data(data: (String, Seq[String])): HadoopPlatformJobTest =
    copy(dataToCreate = dataToCreate :+ data)

  override def source[T](out: TypedSink[T], data: Seq[T]): HadoopPlatformJobTest =
    copy(sourceWriters = sourceWriters :+ { args: Args =>
      new Job(args) {
        TypedPipe.from(List("")).flatMap { _ => data }.write(out)
      }
    })

  override def sink[T](in: Mappable[T])(toExpect: (Seq[T]) => Unit): HadoopPlatformJobTest =
    copy(sourceReaders = sourceReaders :+ { m: Mode => toExpect(in.toIterator(Config.defaultFrom(m), m).toSeq) })

  def inspectCompletedFlow(checker: Flow[JobConf] => Unit): HadoopPlatformJobTest =
    copy(flowCheckers = flowCheckers :+ checker)

  override def run(): Unit = {
    System.setProperty("cascading.update.skip", "true")
    val job = init(cons)
    cluster.addClassSourceToClassPath(cons.getClass)
    cluster.addClassSourceToClassPath(job.getClass)
    createSources()
    execute(job)
    checkSinks()
    flowCheckers.foreach { checker =>
      job.completedFlow.collect {
        case f: Flow[JobConf] => checker(f)
      }
    }
  }

  override def init(cons: Args => Job): Job = cons(Mode.putMode(cluster.mode, new Args(argsMap)))

  @annotation.tailrec
  override final def execute(job: Job): Unit = {
    job.run()
    job.clear()
    job.next match {
      case Some(nextJob) => execute(nextJob)
      case None => ()
    }
  }
}
