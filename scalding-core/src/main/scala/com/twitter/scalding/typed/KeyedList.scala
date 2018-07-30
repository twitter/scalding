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
import scala.collection.JavaConverters._

import com.twitter.algebird.{ Fold, Semigroup, Ring, Aggregator }
import com.twitter.algebird.mutable.PriorityQueueMonoid

import com.twitter.scalding.typed.functions._

object KeyedListLike {
  /** KeyedListLike items are implicitly convertable to TypedPipe */
  implicit def toTypedPipe[K, V, S[K, +V] <: KeyedListLike[K, V, S]](keyed: KeyedListLike[K, V, S]): TypedPipe[(K, V)] = keyed.toTypedPipe

  implicit def toTypedPipeKeyed[K, V, S[K, +V] <: KeyedListLike[K, V, S]](keyed: KeyedListLike[K, V, S]): TypedPipe.Keyed[K, V] =
    new TypedPipe.Keyed(keyed.toTypedPipe)
}

/**
 * This is for the case where you don't want to expose any structure
 * but the ability to operate on an iterator of the values
 */
trait KeyedList[K, +T] extends KeyedListLike[K, T, KeyedList]

/**
 * Represents sharded lists of items of type T
 * There are exactly two fundamental operations:
 * toTypedPipe: marks the end of the grouped-on-key operations.
 * mapValueStream: further transforms all values, in order, one at a time,
 *  with a function from Iterator to another Iterator
 */
trait KeyedListLike[K, +T, +This[K, +T] <: KeyedListLike[K, T, This]] extends Serializable {

  /**
   * End of the operations on values. From this point on the keyed structure
   * is lost and another shuffle is generally required to reconstruct it
   */
  def toTypedPipe: TypedPipe[(K, T)]

  /**
   * This is like take except that the items are kept in memory
   * and we attempt to partially execute on the mappers if possible
   * For very large values of n, this could create memory pressure.
   * (as you may aggregate n items in a memory heap for each key)
   * If you get OOM issues, try to resolve using the method `take` instead.
   */
  def bufferedTake(n: Int): This[K, T]
  /*
    Here is an example implementation, but since each subclass of
    KeyedListLike has its own constaints, this is always to be
    overriden.

    {@code

    if (n < 1) {
      // This means don't take anything, which is legal, but strange
      filterKeys(Constant(false))
    } else if (n == 1) {
      head
    } else {
      // By default, there is no ordering. This method is overridden
      // in IdentityValueSortedReduce
      // Note, this is going to bias toward low hashcode items.
      // If you care which items you take, you should sort by a random number
      // or the value itself.
      val fakeOrdering: Ordering[T] = Ordering.by { v: T => v.hashCode }
      implicit val mon = new PriorityQueueMonoid(n)(fakeOrdering)
      mapValues(mon.build(_))
        // Do the heap-sort on the mappers:
        .sum
        .mapValues { vs => vs.iterator.asScala }
        .flattenValues
    }

    }
    */

  /**
   * filter keys on a predicate. More efficient than filter if you are
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

  /**
   * Operate on an Iterator[T] of all the values for each key at one time.
   * Prefer this to toList, when you can avoid accumulating the whole list in memory.
   * Prefer sum, which is partially executed map-side by default.
   * Use mapValueStream when you don't care about the key for the group.
   *
   * Iterator is always Non-empty.
   * Note, any key that has all values removed will not appear in subsequent
   * .mapGroup/mapValueStream
   */
  def mapGroup[V](smfn: (K, Iterator[T]) => Iterator[V]): This[K, V]

  ///////////
  /// The below are all implemented in terms of the above:
  ///////////

  /**
   * Use Algebird Aggregator to do the reduction
   */
  def aggregate[B, C](agg: Aggregator[T, B, C]): This[K, C] =
    mapValues[B](AggPrepare(agg))
      .sum[B](agg.semigroup)
      .mapValues[C](AggPresent(agg))

  /**
   * .filter(fn).toTypedPipe == .toTypedPipe.filter(fn)
   * It is generally better to avoid going back to a TypedPipe
   * as long as possible: this minimizes the times we go in
   * and out of cascading/hadoop types.
   */
  def filter(fn: ((K, T)) => Boolean): This[K, T] =
    mapGroup(FilterGroup(fn))

  /**
   * flatten the values
   * Useful after sortedTake, for instance
   */
  def flattenValues[U](implicit ev: T <:< TraversableOnce[U]): This[K, U] =
    flatMapValues(Widen(SubTypes.fromEv(ev)))

  /**
   * This is just short hand for mapValueStream(identity), it makes sure the
   * planner sees that you want to force a shuffle. For expert tuning
   */
  def forceToReducers: This[K, T] =
    mapValueStream(Identity())

  /**
   * Use this to get the first value encountered.
   * prefer this to take(1).
   */
  def head: This[K, T] = sum(HeadSemigroup[T]())

  /**
   * This is a special case of mapValueStream, but can be optimized because it doesn't need
   * all the values for a given key at once.  An unoptimized implementation is:
   * mapValueStream { _.map { fn } }
   * but for Grouped we can avoid resorting to mapValueStream
   */
  def mapValues[V](fn: T => V): This[K, V] =
    mapGroup(MapGroupMapValues(fn))

  /**
   * Similar to mapValues, but works like flatMap, returning a collection of outputs
   * for each value input.
   */
  def flatMapValues[V](fn: T => TraversableOnce[V]): This[K, V] =
    mapGroup(MapGroupFlatMapValues(fn))

  /**
   * Use this when you don't care about the key for the group,
   * otherwise use mapGroup
   */
  def mapValueStream[V](smfn: Iterator[T] => Iterator[V]): This[K, V] =
    mapGroup(MapValueStream(smfn))

  /**
   * Add all items according to the implicit Semigroup
   * If there is no sorting, we default to assuming the Semigroup is
   * commutative. If you don't want that, define an ordering on the Values,
   * sort or .forceToReducers.
   *
   * Semigroups MAY have a faster implementation of sum for iterators,
   * so prefer using sum/sumLeft to reduce
   */
  def sum[U >: T](implicit sg: Semigroup[U]): This[K, U] = sumLeft[U]

  /**
   * reduce with fn which must be associative and commutative.
   * Like the above this can be optimized in some Grouped cases.
   * If you don't have a commutative operator, use reduceLeft
   */
  def reduce[U >: T](fn: (U, U) => U): This[K, U] =
    sum(SemigroupFromFn(fn))

  /**
   * Take the largest k things according to the implicit ordering.
   * Useful for top-k without having to call ord.reverse
   */
  def sortedReverseTake[U >: T](k: Int)(implicit ord: Ordering[U]): This[K, Seq[U]] =
    sortedTake[U](k)(ord.reverse)

  /**
   * This implements bottom-k (smallest k items) on each mapper for each key, then
   * sends those to reducers to get the result. This is faster
   * than using .take if k * (number of Keys) is small enough
   * to fit in memory.
   */
  def sortedTake[U >: T](k: Int)(implicit ord: Ordering[U]): This[K, Seq[U]] = {
    val mon = new PriorityQueueMonoid[U](k)(ord)
    mapValues(mon.build(_))
      .sum(mon) // results in a PriorityQueue
      // scala can't infer the type, possibly due to the view bound on TypedPipe
      .mapValues(_.iterator.asScala.toList.sorted(ord))
  }

  /** Like the above, but with a less than operation for the ordering */
  def sortWithTake[U >: T](k: Int)(lessThan: (U, U) => Boolean): This[K, Seq[T]] =
    sortedTake(k)(Ordering.fromLessThan(lessThan))

  /** For each key, Return the product of all the values */
  def product[U >: T](implicit ring: Ring[U]): This[K, U] =
    sum(SemigroupFromProduct(ring))

  /** For each key, count the number of values that satisfy a predicate */
  def count(fn: T => Boolean): This[K, Long] =
    mapValues(Count(fn)).sum

  /** For each key, check to see if a predicate is true for all Values*/
  def forall(fn: T => Boolean): This[K, Boolean] =
    mapValues(fn).product

  /**
   * For each key, selects all elements except first n ones.
   */
  def drop(n: Int): This[K, T] =
    mapValueStream(Drop(n))

  /**
   * For each key, Drops longest prefix of elements that satisfy the given predicate.
   */
  def dropWhile(p: T => Boolean): This[K, T] =
    mapValueStream(DropWhile(p))

  /**
   * For each key, Selects first n elements. Don't use this if n == 1, head is faster in that case.
   */
  def take(n: Int): This[K, T] =
    if (n < 1) filterKeys(Constant(false)) // just don't keep anything
    else if (n == 1) head
    else mapValueStream(Take(n))

  /**
   * For each key, Takes longest prefix of elements that satisfy the given predicate.
   */
  def takeWhile(p: T => Boolean): This[K, T] =
    mapValueStream(TakeWhile(p))

  /**
   * Folds are composable aggregations that make one pass over the data.
   * If you need to do several custom folds over the same data, use Fold.join
   * and this method
   */
  def fold[V](f: Fold[T, V]): This[K, V] =
    mapValueStream(FoldIterator(f))

  /**
   * If the fold depends on the key, use this method to construct
   * the fold for each key
   */
  def foldWithKey[V](fn: K => Fold[T, V]): This[K, V] =
    mapGroup(FoldWithKeyIterator(fn))

  /** For each key, fold the values. see scala.collection.Iterable.foldLeft */
  def foldLeft[B](z: B)(fn: (B, T) => B): This[K, B] =
    mapValueStream(FoldLeftIterator(z, fn))

  /** For each key, scanLeft the values. see scala.collection.Iterable.scanLeft */
  def scanLeft[B](z: B)(fn: (B, T) => B): This[K, B] =
    mapValueStream(ScanLeftIterator(z, fn))

  /**
   * Similar to reduce but always on the reduce-side (never optimized to mapside),
   * and named for the scala function. fn need not be associative and/or commutative.
   * Makes sense when you want to reduce, but in a particular sorted order.
   * the old value comes in on the left.
   */
  def reduceLeft[U >: T](fn: (U, U) => U): This[K, U] =
    sumLeft[U](SemigroupFromFn(fn))

  /**
   * Semigroups MAY have a faster implementation of sum for iterators,
   * so prefer using sum/sumLeft to reduce/reduceLeft
   */
  def sumLeft[U >: T](implicit sg: Semigroup[U]): This[K, U] =
    mapValueStream[U](SumAll(sg))

  /** For each key, give the number of values */
  def size: This[K, Long] = mapValues(Constant(1L)).sum

  /**
   * For each key, give the number of unique values. WARNING: May OOM.
   * This assumes the values for each key can fit in memory.
   */
  def distinctSize: This[K, Long] =
    toSet[T].mapValues(SizeOfSet())

  /**
   * For each key, remove duplicate values. WARNING: May OOM.
   * This assumes the values for each key can fit in memory.
   */
  def distinctValues: This[K, T] = toSet[T].flattenValues

  /**
   * AVOID THIS IF POSSIBLE
   * For each key, accumulate all the values into a List. WARNING: May OOM
   * Only use this method if you are sure all the values will fit in memory.
   * You really should try to ask why you need all the values, and if you
   * want to do some custom reduction, do it in mapGroup or mapValueStream
   *
   * This does no map-side aggregation even though it is a Monoid because
   * toList does not decrease the size of the data at all, so in practice
   * it only wastes effort to try to cache.
   */
  def toList: This[K, List[T]] = mapValueStream(ToList[T]())
  /**
   * AVOID THIS IF POSSIBLE
   * Same risks apply here as to toList: you may OOM. See toList.
   * Note that toSet needs to be parameterized even though toList does not.
   * This is because List is covariant in its type parameter in the scala API,
   * but Set is invariant.  See:
   * http://stackoverflow.com/questions/676615/why-is-scalas-immutable-set-not-covariant-in-its-type
   */
  def toSet[U >: T]: This[K, Set[U]] = mapValues(ToSet[U]()).sum

  /** For each key, give the maximum value*/
  def max[B >: T](implicit cmp: Ordering[B]): This[K, T] =
    reduce(MaxOrd[T, B](cmp))

  /** For each key, give the maximum value by some function*/
  def maxBy[B](fn: T => B)(implicit cmp: Ordering[B]): This[K, T] =
    reduce(MaxOrdBy(fn, cmp))

  /** For each key, give the minimum value*/
  def min[B >: T](implicit cmp: Ordering[B]): This[K, T] =
    reduce(MinOrd[T, B](cmp))

  /** For each key, give the minimum value by some function*/
  def minBy[B](fn: T => B)(implicit cmp: Ordering[B]): This[K, T] =
    reduce(MinOrdBy(fn, cmp))


  /** Use this to error if there is more than 1 value per key
   *  Using this makes it easier to detect when data does
   *  not have the shape you expect and to communicate to
   *  scalding that certain optimizations are safe to do
   *
   *  Note, this has no effect and is a waste to call
   *  after sum because it is true by construction at that
   *  point
   */
  def requireSingleValuePerKey: This[K, T] =
    mapValueStream(SumAll(RequireSingleSemigroup()))

  /** Convert to a TypedPipe and only keep the keys */
  def keys: TypedPipe[K] = toTypedPipe.keys
  /** Convert to a TypedPipe and only keep the values */
  def values: TypedPipe[T] = toTypedPipe.values
}
