package com.twitter.scalding.dagon

import java.io.Serializable
/**
 * This is a useful cache for memoizing natural transformations.
 *
 * The cache is implemented using a mutable pointer to an immutable
 * map value. In the worst-case, race conditions might cause us to
 * lose cache values (i.e. compute some keys twice), but we will never
 * produce incorrect values.
 */
sealed class HCache[K[_], V[_]] private (init: HMap[K, V]) extends Serializable {

  private[this] var hmap: HMap[K, V] = init

  /**
   * Given a key, either return a cached value, or compute, store, and
   * return a new value.
   *
   * This method is what justifies the existence of Cache. Its second
   * parameter (`v`) is by-name: it will only be evaluated in cases
   * where the key is not cached.
   *
   * For example:
   *
   *     def greet(i: Int): Option[Int] = {
   *       println("hi")
   *       Option(i + 1)
   *     }
   *
   *     val c = Cache.empty[Option, Option]
   *     c.getOrElseUpdate(Some(1), greet(1)) // says hi, returns Some(2)
   *     c.getOrElseUpdate(Some(1), greet(1)) // just returns Some(2)
   */
  def getOrElseUpdate[T](k: K[T], v: => V[T]): V[T] =
    hmap.get(k) match {
      case Some(exists) => exists
      case None =>
        val res = v
        hmap = hmap + (k -> res)
        res
    }

  /**
   * Create a second cache with the same values as this one.
   *
   * The two caches will start with the same values, but will be
   * independently updated.
   */
  def duplicate: HCache[K, V] =
    new HCache(hmap)

  /**
   * Access the currently-cached keys and values as a map.
   */
  def toHMap: HMap[K, V] =
    hmap

  /**
   * Forget all cached keys and values.
   *
   * After calling this method, the resulting cache is equivalent to
   * Cache.empty[K, V].
   */
  def reset(): Unit =
    hmap = HMap.empty[K, V]
}

object HCache {
  def empty[K[_], V[_]]: HCache[K, V] = new HCache(HMap.empty)
}
