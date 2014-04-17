/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding.typed

import java.io.Serializable
import java.util.PriorityQueue
import scala.collection.JavaConverters._

import com.twitter.algebird.{Semigroup, Ring, Aggregator}
import com.twitter.algebird.mutable.PriorityQueueMonoid

import com.twitter.scalding._

object KeyedListLike {
  /** KeyedListLike items are implicitly convertable to TypedPipe */
  implicit def toTypedPipe[K,V,S[K,+V] <: KeyedListLike[K,V,S]]
    (keyed: KeyedListLike[K, V, S]): TypedPipe[(K, V)] = keyed.toTypedPipe
}

/** This is for the case where you don't want to expose any structure
 * but the ability to operate on an iterator of the values
 */
trait KeyedList[K, +T] extends KeyedListLike[K,T,KeyedList]

/** Represents sharded lists of items of type T
 * There are exactly two the fundamental operations:
 * toTypedPipe: marks the end of the grouped-on-key operations.
 * mapValueStream: further transforms all values, in order, one at a time,
 *  with a function from Iterator to another Iterator
 */
trait KeyedListLike[K, +T, +This[K,+T] <: KeyedListLike[K,T,This]]
  extends java.io.Serializable {

  /** End of the operations on values. From this point on the keyed structure
   * is lost and another shuffle is generally required to reconstruct it
   */
  def toTypedPipe: TypedPipe[(K, T)]

  /** filter keys on a predicate. More efficient than filter if you are
   * only looking at keys
   */
  def filterKeys(fn: K => Boolean): This[K, T]
  /* an inefficient implementation is below, but
   * since this can always be pushed mapside, we should avoid
   * using this implementation, lest we accidentally forget to
   * implement the smart thing
   * {@code
   *   mapGroup { (k: K, items: Iterator[T]) => if (fn(k)) items else Iterator.empty }
   * }
   */


  /** Operate on an Iterator[T] of all the values for each key at one time.
   * Avoid accumulating the whole list in memory if you can.  Prefer sum,
   * which is partially executed map-side by default.
   */
  def mapGroup[V](smfn : (K, Iterator[T]) => Iterator[V]): This[K, V]

  ///////////
  /// The below are all implemented in terms of the above:
  ///////////

  /** Use Algebird Aggregator to do the reduction
   */
  def aggregate[B,C](agg: Aggregator[T,B,C]): This[K,C] =
    mapValues[B](agg.prepare(_))
      .reduce[B](agg.reduce _)
      .mapValues[C](agg.present(_))

  /** .filter(fn).toTypedPipe == .toTypedPipe.filter(fn)
   * It is generally better to avoid going back to a TypedPipe
   * as long as possible: this minimizes the times we go in
   * and out of cascading/hadoop types.
   */
  def filter(fn: ((K, T)) => Boolean): This[K, T] =
    mapGroup { (k: K, items: Iterator[T]) => items.filter { t => fn((k, t)) } }

  /** flatten the values
   * Useful after sortedTake, for instance
   */
  def flattenValues[U](implicit ev: T <:< TraversableOnce[U]): This[K, U] =
    mapValueStream(_.flatMap { us => us.asInstanceOf[TraversableOnce[U]] })

  /** This is just short hand for mapValueStream(identity), it makes sure the
   * planner sees that you want to force a shuffle. For expert tuning
   */
  def forceToReducers: This[K,T] =
    mapValueStream(identity)

  /** Use this to get the first value encountered.
   * prefer this to take(1).
   */
  def head: This[K, T] = sum {
    new Semigroup[T] {
      override def plus(left: T, right: T) = left
      // Don't enumerate every item, just take the first
      override def sumOption(to: TraversableOnce[T]): Option[T] =
        if(to.isEmpty) None
        else Some(to.toIterator.next)
    }
  }

  /** This is a special case of mapValueStream, but can be optimized because it doesn't need
   * all the values for a given key at once.  An unoptimized implementation is:
   * mapValueStream { _.map { fn } }
   * but for Grouped we can avoid resorting to mapValueStream
   */
  def mapValues[V](fn : T => V): This[K, V] =
    mapGroup { (_, iter) => iter.map(fn) }

  /** Use this when you don't care about the key for the group,
   * otherwise use mapGroup
   */
  def mapValueStream[V](smfn : Iterator[T] => Iterator[V]): This[K, V] =
    mapGroup { (k: K, items: Iterator[T]) => smfn(items) }

  /** Add all items according to the implicit Semigroup
   * If there is no sorting, we default to assuming the Semigroup is
   * commutative. If you don't want that, define an ordering on the Values,
   * sort or .forceToReducers.
   *
   * Semigroups MAY have a faster implementation of sum for iterators,
   * so prefer using sum/sumLeft to reduce
   */
  def sum[U >: T](implicit sg: Semigroup[U]): This[K, U] = sumLeft[U]

  /** reduce with fn which must be associative and commutative.
   * Like the above this can be optimized in some Grouped cases.
   * If you don't have a commutative operator, use reduceLeft
   */
  def reduce[U >: T](fn : (U,U) => U): This[K, U] = sum(Semigroup.from(fn))

  /** Take the largest k things according to the implicit ordering.
   * Useful for top-k without having to call ord.reverse
   */
  def sortedReverseTake(k: Int)(implicit ord: Ordering[_ >: T]): This[K, Seq[T]] =
    sortedTake(k)(ord.reverse)

  /** This implements bottom-k (smallest k items) on each mapper for each key, then
   * sends those to reducers to get the result. This is faster
   * than using .take if k * (number of Keys) is small enough
   * to fit in memory.
   */
  def sortedTake(k: Int)(implicit ord: Ordering[_ >: T]): This[K, Seq[T]] = {
    // cast because Ordering is not contravariant, but could be (and this cast is safe)
    val ordT: Ordering[T] = ord.asInstanceOf[Ordering[T]]
    val mon = new PriorityQueueMonoid[T](k)(ordT)
    mapValues(mon.build(_))
      .sum(mon) // results in a PriorityQueue
      // scala can't infer the type, possibly due to the view bound on TypedPipe
      .mapValues(_.iterator.asScala.toList.sorted(ordT))
  }

  /** Like the above, but with a less than operation for the ordering */
  def sortWithTake[U >: T](k: Int)(lessThan: (U, U) => Boolean): This[K, Seq[T]] =
    sortedTake(k)(Ordering.fromLessThan(lessThan))

  /** For each key, Return the product of all the values */
  def product[U >: T](implicit ring : Ring[U]): This[K, U] = reduce(ring.times)

  /** For each key, count the number of values that satisfy a predicate */
  def count(fn : T => Boolean) : This[K, Long] =
    mapValues { t => if (fn(t)) 1L else 0L }.sum

  /** For each key, check to see if a predicate is true for all Values*/
  def forall(fn : T => Boolean): This[K, Boolean] =
    mapValues { fn(_) }.product

  /** For each key, selects all elements except first n ones.
   */
  def drop(n: Int): This[K, T] =
    mapValueStream { _.drop(n) }

  /** For each key, Drops longest prefix of elements that satisfy the given predicate.
   */
  def dropWhile(p: (T) => Boolean): This[K, T] =
     mapValueStream {_.dropWhile(p)}

  /** For each key, Selects first n elements. Don't use this if n == 1, head is faster in that case.
   */
  def take(n: Int): This[K, T] =
    mapValueStream {_.take(n)}

  /** For each key, Takes longest prefix of elements that satisfy the given predicate.
   */
  def takeWhile(p: (T) => Boolean): This[K, T] =
    mapValueStream {_.takeWhile(p)}

  /** For each key, fold the values. see scala.collection.Iterable.foldLeft */
  def foldLeft[B](z : B)(fn : (B,T) => B): This[K, B] =
    mapValueStream { stream => Iterator(stream.foldLeft(z)(fn)) }

  /** For each key, scanLeft the values. see scala.collection.Iterable.scanLeft */
  def scanLeft[B](z : B)(fn : (B,T) => B): This[K, B] =
    mapValueStream { _.scanLeft(z)(fn) }

  /** Similar to reduce but always on the reduce-side (never optimized to mapside),
   * and named for the scala function. fn need not be associative and/or commutative.
   * Makes sense when you want to reduce, but in a particular sorted order.
   * the old value comes in on the left.
   */
  def reduceLeft[U >: T](fn : (U,U) => U): This[K, U] =
    sumLeft[U](Semigroup.from(fn))

  /** Semigroups MAY have a faster implementation of sum for iterators,
   * so prefer using sum/sumLeft to reduce/reduceLeft
   */
  def sumLeft[U >: T](implicit sg: Semigroup[U]): This[K, U] =
    mapValueStream[U](Semigroup.sumOption[U](_).iterator)

  /** For each key, give the number of values */
  def size : This[K,Long] = mapValues { x => 1L }.sum
  /** AVOID THIS IF POSSIBLE
   * For each key, accumulate all the values into a List. WARNING: May OOM
   * Only use this method if you are sure all the values will fit in memory.
   * You really should try to ask why you need all the values, and if you
   * want to do some custom reduction, do it in mapGroup or mapValueStream
   */
  def toList : This[K,List[T]] = mapValues { List(_) }.sum
  /** AVOID THIS IF POSSIBLE
   * Same risks apply here as to toList: you may OOM. See toList.
   * Note that toSet needs to be parameterized even though toList does not.
   * This is because List is covariant in its type parameter in the scala API,
   * but Set is invariant.  See:
   * http://stackoverflow.com/questions/676615/why-is-scalas-immutable-set-not-covariant-in-its-type
   */
  def toSet[U >: T] : This[K,Set[U]] = mapValues { Set[U](_) }.sum

  /** For each key, give the maximum value*/
  def max[B >: T](implicit cmp : Ordering[B]): This[K, T] =
    reduce(cmp.max).asInstanceOf[This[K, T]]

  /** For each key, give the maximum value by some function*/
  def maxBy[B](fn : T => B)(implicit cmp : Ordering[B]): This[K, T] =
    reduce(Ordering.by(fn).max)

  /** For each key, give the minimum value*/
  def min[B >: T](implicit cmp: Ordering[B]): This[K, T] =
    reduce(cmp.min).asInstanceOf[This[K,T]]

  /** For each key, give the minimum value by some function*/
  def minBy[B](fn : T => B)(implicit cmp: Ordering[B]): This[K,T] =
    reduce(Ordering.by(fn).min)

  /** Convert to a TypedPipe and only keep the keys */
  def keys: TypedPipe[K] = toTypedPipe.keys
  /** Convert to a TypedPipe and only keep the values */
  def values: TypedPipe[T] = toTypedPipe.values
}
