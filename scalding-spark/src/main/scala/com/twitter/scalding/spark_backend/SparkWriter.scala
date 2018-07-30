package com.twitter.scalding.spark_backend

import scala.concurrent.{ Future, ExecutionContext }
import com.twitter.scalding.typed._
import com.twitter.scalding.{ Config, Execution, ExecutionCounters }

import Execution.{ ToWrite, Writer }

class SparkWriter(sparkMode: SparkMode) extends Writer {
  def execute(conf: Config, writes: List[ToWrite[_]])(implicit cec: ExecutionContext): Future[(Long, ExecutionCounters)] = ???
  def finished(): Unit = ???

  def getForced[T](conf: Config, initial: TypedPipe[T])(implicit cec: ExecutionContext): Future[TypedPipe[T]] = ???
  def getIterable[T](conf: Config, initial: TypedPipe[T])(implicit cec: ExecutionContext): Future[Iterable[T]] = ???
  def start(): Unit = ???
}
