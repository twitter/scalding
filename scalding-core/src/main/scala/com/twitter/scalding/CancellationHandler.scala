package com.twitter.scalding

import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext }

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

  def fromFn(fn: ConcurrentExecutionContext => Future[Unit]): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = fn(ec)
  }

  def fromFuture(f: Future[CancellationHandler]): CancellationHandler = new CancellationHandler {
    override def stop()(implicit ec: ConcurrentExecutionContext): Future[Unit] = {
      f.flatMap(_.stop())
    }
  }
}