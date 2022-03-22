package com.twitter.scalding.dagon

object ScalaVersionCompat {
  type LazyList[+A] = scala.collection.immutable.LazyList[A]
  val LazyList = scala.collection.immutable.LazyList

  type IterableOnce[+A] = scala.collection.IterableOnce[A]

  def iterateOnce[A](as: IterableOnce[A]): Iterator[A] =
    as.iterator

  def lazyList[A](as: A*): LazyList[A] =
    LazyList(as: _*)

  def lazyListToIterator[A](lst: LazyList[A]): Iterator[A] =
    lst.iterator

  def lazyListFromIterator[A](it: Iterator[A]): LazyList[A] =
    LazyList.from(it)

  implicit val ieeeDoubleOrdering: Ordering[Double] =
    Ordering.Double.IeeeOrdering
}
