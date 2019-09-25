package com.twitter.scalding

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ Future, Promise, ExecutionContext => ConcurrentExecutionContext }

trait PromiseLike[P[_], F[_]] {
  def apply[T](): P[T]
  def future[T](p: P[T]): F[T]
  def completeWith[T](p: P[T], other: F[T]): P[T]
}

object PromiseLike {
  implicit object PromiseLikeCPromise extends PromiseLike[CPromise, CFuture] {
    def apply[T](): CPromise[T] = CPromise[T]()
    def future[T](p: CPromise[T]): CFuture[T] = p.cfuture
    def completeWith[T](p: CPromise[T], other: CFuture[T]): CPromise[T] = p.completeWith(other)
  }

  implicit object PromiseLikePromise extends PromiseLike[Promise, Future] {
    def apply[T](): Promise[T] = Promise[T]()
    def future[T](p: Promise[T]): Future[T] = p.future
    def completeWith[T](p: Promise[T], other: Future[T]): Promise[T] = p.completeWith(other)
  }
}

/**
 * This is a map for values that are produced in futures
 * as is common in Execution
 */
class FutureCacheGeneric[-K, V, P[_], F[_]](implicit pl: PromiseLike[P, F]) {
  private[this] val cache = new ConcurrentHashMap[K, F[V]]()

  def get(k: K): Option[F[V]] = Option(cache.get(k))

  def getOrElseUpdate(k: K, res: => F[V]): F[V] =
    getOrElseUpdateIsNew(k, res)._2

  /**
   * Tells you if this was the first lookup of this key or not
   */
  def getOrElseUpdateIsNew(k: K, res: => F[V]): (Boolean, F[V]) =
    getOrPromise(k) match {
      case Left(cpromise) =>
        // be careful to not evaluate res twice
        pl.completeWith(cpromise, res)
        (true, pl.future(cpromise))
      case Right(cfut) => (false, cfut)
    }

  /**
   * If you get a Left value as a result you MUST complete that Promise
   * or you may deadlock other callers
   */
  def getOrPromise(k: K): Either[P[V], F[V]] = {
    /*
     * Since we don't want to evaluate res twice, we make a promise
     * which we will use if it has not already been evaluated
     */
    val cpromise = pl.apply[V]()
    val cancelFut = pl.future(cpromise)

    cache.putIfAbsent(k, cancelFut) match {
      case null => Left(cpromise)
      case existsFut => Right(existsFut)
    }
  }
}

class FutureCache[-K, V] extends FutureCacheGeneric[K, V, Promise, Future]()(PromiseLike.PromiseLikePromise)
