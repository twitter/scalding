package com.twitter.scalding.dagon

import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong

/**
 * The Expressions are assigned Ids. Each Id is associated with an expression of inner type T.
 *
 * This is done to put an indirection in the Dag that allows us to rewrite nodes by simply replacing the
 * expressions associated with given Ids.
 *
 * T is a phantom type used by the type system
 */
final class Id[T] private (val serial: Long) extends Serializable {
  require(serial >= 0, s"counter overflow has occurred: $serial")
  override def toString: String = s"Id($serial)"
}

object Id {

  @transient private[this] val counter = new AtomicLong(0)

  def next[T](): Id[T] =
    new Id[T](counter.getAndIncrement())

  implicit def idOrdering[T]: Ordering[Id[T]] =
    new Ordering[Id[T]] {
      def compare(a: Id[T], b: Id[T]) =
        java.lang.Long.compare(a.serial, b.serial)
    }
}
