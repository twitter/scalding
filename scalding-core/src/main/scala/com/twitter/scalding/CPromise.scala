package com.twitter.scalding

import scala.concurrent.{ExecutionContext => ConcurrentExecutionContext, Future, Promise}

/**
 * Represents a cancellable promise.
 */
case class CPromise[T](promise: Promise[T], cancellationHandler: Promise[CancellationHandler]) {

  /**
   * Creates a CFuture using the given promises.
   */
  def cfuture: CFuture[T] =
    CFuture(promise.future, CancellationHandler.fromFuture(cancellationHandler.future))

  def completeWith(other: CFuture[T]): this.type = {
    // fulfill the main and cancellation handler promises
    promise.completeWith(other.future)
    cancellationHandler.completeWith(Future.successful(other.cancellationHandler))
    this
  }
}
object CPromise {
  def apply[T](): CPromise[T] = CPromise(Promise[T](), Promise[CancellationHandler]())
}
