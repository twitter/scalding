package com.twitter.scalding.typed.memory_backend

import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext }
import com.twitter.scalding.{ Execution, Mode }
import com.twitter.scalding.typed._
import Execution.Writer

final case class MemoryMode(srcs: Resolver[TypedSource, MemorySource], sinks: Resolver[TypedSink, MemorySink]) extends Mode {

  def newWriter(): Writer =
    new MemoryWriter(this)

  /**
   * Add a new source resolver whose sources take precedence over any currently registered
   * sources
   */
  def addSourceResolver(res: Resolver[TypedSource, MemorySource]): MemoryMode =
    MemoryMode(res.orElse(srcs), sinks)

  def addSource[T](src: TypedSource[T], ts: MemorySource[T]): MemoryMode =
    addSourceResolver(Resolver.pair(src, ts))

  def addSourceFn[T](src: TypedSource[T])(fn: ConcurrentExecutionContext => Future[Iterator[T]]): MemoryMode =
    addSource(src, MemorySource.Fn(fn))

  def addSourceIterable[T](src: TypedSource[T], iter: Iterable[T]): MemoryMode =
    addSource(src, MemorySource.FromIterable(iter))

  /**
   * Add a new sink resolver whose sinks take precedence over any currently registered
   * sinks
   */
  def addSinkResolver(res: Resolver[TypedSink, MemorySink]): MemoryMode =
    MemoryMode(srcs, res.orElse(sinks))

  def addSink[T](sink: TypedSink[T], msink: MemorySink[T]): MemoryMode =
    addSinkResolver(Resolver.pair(sink, msink))

  /**
   * This has a side effect of mutating the corresponding MemorySink
   */
  def writeSink[T](t: TypedSink[T], iter: Iterable[T])(implicit ec: ConcurrentExecutionContext): Future[Unit] =
    sinks(t) match {
      case Some(sink) => sink.write(iter)
      case None => Future.failed(new Exception(s"missing sink for $t, with first 10 values to write: ${iter.take(10).toList.toString}..."))
    }
}

object MemoryMode {
  def empty: MemoryMode =
    apply(Resolver.empty[TypedSource, MemorySource], Resolver.empty[TypedSink, MemorySink])
}

