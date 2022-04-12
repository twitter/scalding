package com.twitter.scalding.dagon

import scala.util.hashing.MurmurHash3

/**
 * A tuple2 that uses reference equality on items to do equality useful for caching the results of pair-wise
 * functions on DAGs.
 *
 * Without this, you can easily get exponential complexity on recursion on DAGs.
 */
case class RefPair[A <: AnyRef, B <: AnyRef](_1: A, _2: B) {

  override lazy val hashCode: Int = MurmurHash3.productHash(this)

  override def equals(that: Any) = that match {
    case RefPair(thatA, thatB) => (_1 eq thatA) && (_2 eq thatB)
    case _                     => false
  }

  /**
   * true if the left is referentially equal to the right
   */
  def itemsEq: Boolean = _1 eq _2
}
