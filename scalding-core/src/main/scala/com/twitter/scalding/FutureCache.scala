package com.twitter.scalding

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ Future, Promise, ExecutionContext => ConcurrentExecutionContext }
/**
 * This is a map for values that are produced in futures
 * as is common in Execution
 */
class FutureCache[-K, V] {
  private[this] val cache = new ConcurrentHashMap[K, (Future[V], CancellationHandler)]()

  def get(k: K): Option[(Future[V], CancellationHandler)] = Option(cache.get(k))

  def getOrElseUpdate(k: K, res: => (Future[V], CancellationHandler))(implicit cec: ConcurrentExecutionContext): (Future[V], CancellationHandler) =
    getOrElseUpdateIsNew(k, res)._2

  /**
   * Tells you if this was the first lookup of this key or not
   */
  def getOrElseUpdateIsNew(k: K, res: => (Future[V], CancellationHandler))(implicit cec: ConcurrentExecutionContext): (Boolean, (Future[V], CancellationHandler)) =
    getOrPromise(k) match {
      case (Left(promise), cancel) =>
        // be careful to not evaluate res twice
        promise.completeWith(res._1)
        (true, (promise.future, cancel))
      case (Right(fut), cancel) => (false, (fut, cancel))
    }

  /**
   * If you get a Left value as a result you MUST complete that Promise
   * or you may deadlock other callers
   */
  def getOrPromise(k: K)(implicit cec: ConcurrentExecutionContext): (Either[Promise[V], Future[V]], CancellationHandler) = {
    /*
     * Since we don't want to evaluate res twice, we make a promise
     * which we will use if it has not already been evaluated
     */
    val promise = Promise[V]()
    val fut = promise.future
    val cancelHandler = CancellationHandler.empty
    cache.putIfAbsent(k, (fut, cancelHandler)) match {
      case null => (Left(promise), cancelHandler)
      case (existsFut, existsCancel) => (Right(existsFut), existsCancel)
    }
  }
}
