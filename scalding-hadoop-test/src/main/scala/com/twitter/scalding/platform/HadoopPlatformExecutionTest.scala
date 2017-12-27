package com.twitter.scalding.platform

import cascading.flow.Flow
import com.twitter.scalding._
import org.apache.hadoop.mapred.JobConf
import scala.util.{ Failure, Success }

case class HadoopPlatformExecutionTest(
  cons: (Config) => Execution[_],
  cluster: LocalCluster,
  parameters: Map[String, String] = Map.empty,
  dataToCreate: Seq[(String, Seq[String])] = Vector(),
  sourceWriters: Seq[Config => Execution[_]] = Vector.empty,
  sourceReaders: Seq[Mode => Unit] = Vector.empty,
  flowCheckers: Seq[Flow[JobConf] => Unit] = Vector.empty) extends HadoopPlatform[Config, Execution[_], HadoopPlatformExecutionTest] {

  def config: Config =
    Config.defaultFrom(cluster.mode) ++ Config.from(parameters)

  override def arg(key: String, value: String): HadoopPlatformExecutionTest =
    copy(parameters = parameters + (key -> value))

  override def data(data: (String, Seq[String])): HadoopPlatformExecutionTest =
    copy(dataToCreate = dataToCreate :+ data)

  override def source[K](out: TypedSink[K], data: Seq[K]): HadoopPlatformExecutionTest =
    copy(sourceWriters = sourceWriters :+ { config: Config =>
      TypedPipe.from(data).writeExecution(out)
    })

  override def sink[K](in: Mappable[K])(toExpect: (Seq[K]) => Unit): HadoopPlatformExecutionTest =
    copy(sourceReaders = sourceReaders :+ { m: Mode => toExpect(in.toIterator(config, m).toSeq) })

  override def run(): Unit = {
    System.setProperty("cascading.update.skip", "true")
    val execution = init(cons)
    cluster.addClassSourceToClassPath(cons.getClass)
    cluster.addClassSourceToClassPath(execution.getClass)
    createSources()
    execute(execution)
    checkSinks()
  }

  override def init(cons: (Config) => Execution[_]): Execution[_] = cons(config)

  override def execute(unit: Execution[_]): Unit =
    unit.waitFor(config, cluster.mode) match {
      case Success(s) => s
      case Failure(e) => throw e
    }
}