package com.twitter.scalding.typed.memory_backend

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding.typed._
import com.twitter.scalding.Mode
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.util.{Failure, Success}

trait MemorySource[A] {
  def read()(implicit ec: ConcurrentExecutionContext): Future[Iterator[A]]
}

object MemorySource {
  case class FromIterable[A](iter: Iterable[A]) extends MemorySource[A] {
    def read()(implicit ec: ConcurrentExecutionContext) = Future.successful(iter.iterator)
  }
  case class Fn[A](toFn: ConcurrentExecutionContext => Future[Iterator[A]]) extends MemorySource[A] {
    def read()(implicit ec: ConcurrentExecutionContext) = toFn(ec)
  }

  def readOption[T](optSrc: Option[MemorySource[T]], name: String)(implicit ec: ConcurrentExecutionContext): Future[Iterator[T]] =
    optSrc match {
      case Some(src) => src.read()
      case None => Future.failed(new Exception(s"Source: $name not wired. Please provide an input with MemoryMode.addSource"))
    }

}

trait MemorySink[A] {
  def write(data: Iterable[A])(implicit ec: ConcurrentExecutionContext): Future[Unit]
}

object MemorySink {
  /**
   * This is a sink that writes into local memory which you can read out
   * by a future
   *
   * this needs to be reset between each write (so it only works for a single
   * write per Execution)
   */
  class LocalVar[A] extends MemorySink[A] {
    private[this] val box: AtomicBox[Promise[Iterable[A]]] = new AtomicBox(Promise[Iterable[A]]())

    /**
     * This is a future that completes when a write comes. If no write
     * happens before a reset, the future fails
     */
    def read(): Future[Iterable[A]] = box.get().future

    /**
     * This takes the current future and resets the promise
     * making it safe for another write.
     */
    def reset(): Option[Iterable[A]] = {
      val current = box.swap(Promise[Iterable[A]]())
      // if the promise is not set, it never will be, so
      // go ahead and poll now
      //
      // also note we never set this future to failed
      current.future.value match {
        case Some(Success(res)) =>
          Some(res)
        case Some(Failure(err)) =>
          throw new IllegalStateException("We should never reach this because, we only complete with failure below", err)
        case None =>
          // make sure we complete the original future so readers don't block forever
          current.failure(new Exception(s"sink never written to before reset() called $this"))
          None
      }
    }

    def write(data: Iterable[A])(implicit ec: ConcurrentExecutionContext): Future[Unit] =
      Future {
        box.update { p => (p.success(data), ()) }
      }
  }
}

/**
 * These are just used as type markers which are connected
 * to inputs via the MemoryMode
 */
case class SourceT[T](ident: String) extends TypedSource[T] {
  /**
   * note that ??? in scala is the same as not implemented
   *
   * These methods are not needed for use with the Execution API, and indeed
   * don't make sense outside of cascading, but backwards compatibility
   * currently requires them on TypedSource. Ideally we will find another solution
   * to this in the future
   */
  def converter[U >: T] = ???
  def read(implicit flowDef: FlowDef, mode: Mode): Pipe = ???
}

/**
 * These are just used as type markers which are connected
 * to outputs via the MemoryMode
 */
case class SinkT[T](indent: String) extends TypedSink[T] {
  /**
   * note that ??? in scala is the same as not implemented
   *
   * These methods are not needed for use with the Execution API, and indeed
   * don't make sense outside of cascading, but backwards compatibility
   * currently requires them on TypedSink. Ideally we will find another solution
   * to this in the future
   */
  def setter[U <: T] = ???
  def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe = ???
}
