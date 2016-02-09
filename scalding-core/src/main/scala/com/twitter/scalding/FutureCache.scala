package com.twitter.scalding

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ Future, Promise }
/**
 * This is a map for values that are produced in futures
 * as is common in Execution
 */
class FutureCache[-K, V] {
  private[this] val cache = new ConcurrentHashMap[K, Future[V]]()

  def get(k: K): Option[Future[V]] = Option(cache.get(k))

  def getOrElseUpdate(k: K, res: => Future[V]): Future[V] =
    getOrElseUpdateIsNew(k, res)._2

  /**
   * Tells you if this was the first lookup of this key or not
   */
  def getOrElseUpdateIsNew(k: K, res: => Future[V]): (Boolean, Future[V]) =
    getOrPromise(k) match {
      case Left(promise) =>
        // be careful to not evaluate res twice
        promise.completeWith(res)
        (true, promise.future)
      case Right(fut) => (false, fut)
    }

  /**
   * If you get a Left value as a result you MUST complete that Promise
   * or you may deadlock other callers
   */
  def getOrPromise(k: K): Either[Promise[V], Future[V]] = {
    /*
     * Since we don't want to evaluate res twice, we make a promise
     * which we will use if it has not already been evaluated
     */
    val promise = Promise[V]()
    val fut = promise.future
    cache.putIfAbsent(k, fut) match {
      case null => Left(promise)
      case exists => Right(exists)
    }
  }
}

class FutureSoftCache[-K, V] {
  import java.lang.ref.SoftReference

  private[this] val cache = new ConcurrentHashMap[K, SoftReference[Future[V]]]()

  def get(k: K): Option[Future[V]] =
    Option(cache.get(k)).flatMap { ref => Option(ref.get) }

  def getOrElseUpdate(k: K, res: => Future[V]): Future[V] =
    getOrElseUpdateIsNew(k, res)._2

  /**
   * Tells you if this was the first lookup of this key or not
   */
  def getOrElseUpdateIsNew(k: K, res: => Future[V]): (Boolean, Future[V]) =
    getOrPromise(k) match {
      case Left(promise) =>
        // be careful to not evaluate res twice
        promise.completeWith(res)
        (true, promise.future)
      case Right(fut) => (false, fut)
    }

  /**
   * If you get a Left value as a result you MUST complete that Promise
   * or you may deadlock other callers
   */
  def getOrPromise(k: K): Either[Promise[V], Future[V]] = {
    /*
     * Since we don't want to evaluate res twice, we make a promise
     * which we will use if it has not already been evaluated
     */
    val promise = Promise[V]()
    val ref = new SoftReference(promise.future)
    @annotation.tailrec
    def go(oldValue: SoftReference[Future[V]]): Either[Promise[V], Future[V]] =
      oldValue match {
        case null =>
          // Try to put it:
          cache.putIfAbsent(k, ref) match {
            case null => Left(promise)
            case exists => go(exists)
          }
        case exists =>
          val existingFut = exists.get
          existingFut match {
            case null =>
              // We need to re-run
              if (cache.replace(k, oldValue, ref)) Left(promise)
              else go(cache.get(k))
            case nonnull => Right(nonnull)
          }
      }

    go(cache.get(k))
  }
}
