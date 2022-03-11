package com.twitter.scalding.typed.memory_backend

import scala.concurrent.{ExecutionContext => ConcurrentExecutionContext, Future}
import com.twitter.scalding.{Execution, Mode}
import com.twitter.scalding.typed._
import Execution.Writer

final case class MemoryMode(srcs: Resolver[Input, MemorySource], sinks: Resolver[Output, MemorySink])
    extends Mode {

  def newWriter(): Writer =
    new MemoryWriter(this)

  /**
   * Add a new source resolver whose sources take precedence over any currently registered sources
   */
  def addSourceResolver(res: Resolver[Input, MemorySource]): MemoryMode =
    MemoryMode(res.orElse(srcs), sinks)

  def addSource[T](src: Input[T], ts: MemorySource[T]): MemoryMode =
    addSourceResolver(Resolver.pair(src, ts))

  def addSourceFn[T](src: Input[T])(fn: ConcurrentExecutionContext => Future[Iterator[T]]): MemoryMode =
    addSource(src, MemorySource.Fn(fn))

  def addSourceIterable[T](src: Input[T], iter: Iterable[T]): MemoryMode =
    addSource(src, MemorySource.FromIterable(iter))

  /**
   * Add a new sink resolver whose sinks take precedence over any currently registered sinks
   */
  def addSinkResolver(res: Resolver[Output, MemorySink]): MemoryMode =
    MemoryMode(srcs, res.orElse(sinks))

  def addSink[T](sink: Output[T], msink: MemorySink[T]): MemoryMode =
    addSinkResolver(Resolver.pair(sink, msink))

  /**
   * This has a side effect of mutating the corresponding MemorySink
   */
  def writeSink[T](t: Output[T], iter: Iterable[T])(implicit
      ec: ConcurrentExecutionContext
  ): Future[Unit] =
    sinks(t) match {
      case Some(sink) => sink.write(iter)
      case None =>
        Future.failed(
          new Exception(
            s"missing sink for $t, with first 10 values to write: ${iter.take(10).toList.toString}..."
          )
        )
    }
}

object MemoryMode {
  def empty: MemoryMode =
    apply(Resolver.empty[Input, MemorySource], Resolver.empty[Output, MemorySink])
}
