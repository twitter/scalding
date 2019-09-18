package com.twitter.scalding

import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }

trait CancellationHandler { outer =>
  def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit]
  def compose(other: CancellationHandler): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = {
      other.stop().flatMap(_ => outer.stop())
    }
  }
  def compose(other: Future[CancellationHandler]): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = {
      other.flatMap(_.stop).flatMap(_ => outer.stop())
    }
  }
}

object CancellationHandler {
  def empty: CancellationHandler = new CancellationHandler {
    def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = Future(())
  }

  def cancellable[T](f: Future[T], onCancel: => Unit = ())(implicit ec: ConcurrentExecutionContext): (Future[T], CancellationHandler) = {
    val p = Promise[T]
    val first = Future.firstCompletedOf(List(p.future, f))
    val cancel = new CancellationHandler {
      override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = Future {
        first.onFailure { case e => onCancel }
        p.failure(new Exception("Future was cancelled"))
        ()
      }
    }

    (first, cancel)
  }
}