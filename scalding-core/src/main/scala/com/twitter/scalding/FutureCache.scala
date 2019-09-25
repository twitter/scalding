package com.twitter.scalding

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ Future, Promise, ExecutionContext => ConcurrentExecutionContext }

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
