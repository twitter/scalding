package com.twitter.scalding.typed.functions

import java.io.Serializable

/**
 * This is one of 4 core, non composed operations:
 * identity
 * filter
 * map
 * flatMap
 */
sealed abstract class FlatMapping[-A, +B] extends java.io.Serializable
object FlatMapping {
  def filter[A](fn: A => Boolean): FlatMapping[A, A] =
    Filter[A, A](fn, implicitly)

  def filterKeys[K, V](fn: K => Boolean): FlatMapping[(K, V), (K, V)] =
    filter[(K, V)](FilterKeysToFilter(fn))

  final case class Identity[A, B](ev: EqTypes[A, B]) extends FlatMapping[A, B]
  final case class Filter[A, B](fn: A => Boolean, ev: EqTypes[A, B]) extends FlatMapping[A, B]
  final case class Map[A, B](fn: A => B) extends FlatMapping[A, B]
  final case class FlatM[A, B](fn: A => TraversableOnce[B]) extends FlatMapping[A, B]
}

