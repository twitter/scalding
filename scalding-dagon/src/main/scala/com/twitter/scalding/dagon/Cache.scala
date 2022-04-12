package com.twitter.scalding.dagon

import java.io.Serializable

/**
 * This is a useful cache for memoizing function.
 *
 * The cache is implemented using a mutable pointer to an immutable map value. In the worst-case, race
 * conditions might cause us to lose cache values (i.e. compute some keys twice), but we will never produce
 * incorrect values.
 */
sealed class Cache[K, V] private (init: Map[K, V]) extends Serializable {

  private[this] var map: Map[K, V] = init

  /**
   * Given a key, either return a cached value, or compute, store, and return a new value.
   *
   * This method is what justifies the existence of Cache. Its second parameter (`v`) is by-name: it will only
   * be evaluated in cases where the key is not cached.
   *
   * For example:
   *
   * def greet(i: Int): Int = { println("hi") i + 1 }
   *
   * val c = Cache.empty[Int, Int] c.getOrElseUpdate(1, greet(1)) // says hi, returns 2 c.getOrElseUpdate(1,
   * greet(1)) // just returns 2
   */
  def getOrElseUpdate(k: K, v: => V): V =
    map.get(k) match {
      case Some(exists) => exists
      case None =>
        val res = v
        map = map.updated(k, res)
        res
    }

  /**
   * Create a second cache with the same values as this one.
   *
   * The two caches will start with the same values, but will be independently updated.
   */
  def duplicate: Cache[K, V] =
    new Cache(map)

  /**
   * Access the currently-cached keys and values as a map.
   */
  def toMap: Map[K, V] =
    map

  /**
   * Forget all cached keys and values.
   *
   * After calling this method, the resulting cache is equivalent to Cache.empty[K, V].
   */
  def reset(): Unit =
    map = Map.empty
}

object Cache {
  def empty[K, V]: Cache[K, V] = new Cache(Map.empty)
}
