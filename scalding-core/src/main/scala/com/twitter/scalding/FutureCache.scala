package com.twitter.scalding

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ Future, Promise, ExecutionContext => ConcurrentExecutionContext }

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

  /**
   * Use our internal faster failing zip function rather than the standard one due to waiting
   */
  def failFastSequence[T](t: Iterable[CFuture[T]])(implicit cec: ConcurrentExecutionContext): CFuture[List[T]] = {
    t.foldLeft(CFuture.successful(Nil: List[T])) { (f, i) =>
      f.zip(i).map { case (tail, h) => h :: tail }
    }.map(_.reverse)
  }
}

/**
 * Represents a cancellable promise.
 */
case class CPromise[T](promise: Promise[T], cancellationHandler: Promise[CancellationHandler]) {
  /**
   * Creates a CFuture using the given promises.
   */
  def cfuture: CFuture[T] = {
    CFuture(promise.future, CancellationHandler.fromFuture(cancellationHandler.future))
  }

  def completeWith(other: CFuture[T]): this.type = {
    // fullfill the main and cancellation handler promises
    promise.completeWith(other.future)
    cancellationHandler.completeWith(Future.successful(other.cancellationHandler))
    this
  }
}
object CPromise {
  def apply[T](): CPromise[T] = CPromise(Promise[T](), Promise[CancellationHandler]())
}

/**
 * This is a map for values that are produced in futures
 * as is common in Execution
 */
class FutureCache[-K, V] {
  private[this] val cache = new ConcurrentHashMap[K, CFuture[V]]()

  def get(k: K): Option[CFuture[V]] = Option(cache.get(k))

  def getOrElseUpdate(k: K, res: => CFuture[V])(implicit cec: ConcurrentExecutionContext): CFuture[V] =
    getOrElseUpdateIsNew(k, res)._2

  /**
   * Tells you if this was the first lookup of this key or not
   */
  def getOrElseUpdateIsNew(k: K, res: => CFuture[V])(implicit cec: ConcurrentExecutionContext): (Boolean, CFuture[V]) =
    getOrPromise(k) match {
      case Left(cpromise) =>
        // be careful to not evaluate res twice
        cpromise.completeWith(res)
        (true, cpromise.cfuture)
      case Right(cfut) => (false, cfut)
    }

  /**
   * If you get a Left value as a result you MUST complete that Promise
   * or you may deadlock other callers
   */
  def getOrPromise(k: K)(implicit cec: ConcurrentExecutionContext): Either[CPromise[V], CFuture[V]] = {
    /*
     * Since we don't want to evaluate res twice, we make a promise
     * which we will use if it has not already been evaluated
     */
    val cpromise = CPromise[V]()
    val cancelFut = cpromise.cfuture

    cache.putIfAbsent(k, cancelFut) match {
      case null => Left(cpromise)
      case existsFut => Right(existsFut)
    }
  }
}
