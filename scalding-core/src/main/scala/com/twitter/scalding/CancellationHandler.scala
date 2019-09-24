package com.twitter.scalding

import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }

sealed trait CancellationHandler { outer =>
  def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit]
  def compose(other: CancellationHandler): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = {
      other.stop().zip(outer.stop()).map(_ => ())
    }
  }
}

object CancellationHandler {
  val empty: CancellationHandler = new CancellationHandler {
    def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = Future.successful(())
  }

  def fromFn(fn: () => Unit): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = Future {
      fn()
    }
  }

  def fromFuture(f: Future[CancellationHandler]): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = {
      f.flatMap(_.stop())
    }
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