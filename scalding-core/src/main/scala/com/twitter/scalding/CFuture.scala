package com.twitter.scalding

import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext }

/**
 * Represents a cancellable future.
 */
case class CFuture[+T](future: Future[T], cancellationHandler: CancellationHandler) {
  def map[S](fn: T => S)(implicit cec: ConcurrentExecutionContext): CFuture[S] = {
    val mapped = future.map(fn)
    CFuture(mapped, cancellationHandler)
  }

  def mapFuture[S](fn: Future[T] => Future[S]): CFuture[S] = {
    val transformed = fn(future)
    CFuture(transformed, cancellationHandler)
  }

  def zip[U](other: CFuture[U])(implicit cec: ConcurrentExecutionContext): CFuture[(T, U)] = {
    val zippedFut: Future[(T, U)] = Execution.failFastZip(future, other.future)
    val cancelHandler = cancellationHandler.compose(other.cancellationHandler)

    CFuture(zippedFut, cancelHandler)
  }
}

object CFuture {
  def successful[T](result: T): CFuture[T] = {
    CFuture(Future.successful(result), CancellationHandler.empty)
  }

  def failed(t: Throwable): CFuture[Nothing] = {
    val f = Future.failed(t)
    CFuture(f, CancellationHandler.empty)
  }

  def uncancellable[T](fut: Future[T]): CFuture[T] = {
    CFuture(fut, CancellationHandler.empty)
  }

  def fromFuture[T](fut: Future[CFuture[T]])(implicit cec: ConcurrentExecutionContext): CFuture[T] = {
    CFuture(fut.flatMap(_.future), CancellationHandler.fromFuture(fut.map(_.cancellationHandler)))
  }

  /**
   * Use our internal faster failing zip function rather than the standard one due to waiting
   */
  def failFastSequence[T](t: Iterable[CFuture[T]])(implicit cec: ConcurrentExecutionContext): CFuture[List[T]] = {
    t.foldLeft(CFuture.successful(Nil: List[T])) { (f, i) =>
      f.zip(i).map { case (tail, h) => h :: tail }
    }.map(_.reverse)
  }
}
