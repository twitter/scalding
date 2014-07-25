/*
Copyright 2013 Twitter, Inc.

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

import com.twitter.algebird.Semigroup
import com.twitter.scalding.TupleConverter.tuple2Converter
import com.twitter.scalding.TupleSetter.tup2Setter

import com.twitter.scalding._

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields

import Dsl._

/**
 * This encodes the rules that
 * 1) sorting is only possible before doing any reduce,
 * 2) reversing is only possible after sorting.
 * 3) unsorted Groups can be CoGrouped or HashJoined
 *
 * This may appear a complex type, but it makes
 * sure that code won't compile if it breaks the rule
 */
trait Grouped[K, +V]
  extends KeyedListLike[K, V, UnsortedGrouped]
  with HashJoinable[K, V]
  with Sortable[V, ({ type t[+x] = SortedGrouped[K, x] with Reversable[SortedGrouped[K, x]] })#t]
  with WithReducers[Grouped[K, V]]

/**
 * After sorting, we are no longer CoGroupable, and we can only call reverse
 * in the initial SortedGrouped created from the Sortable:
 * .sortBy(_._2).reverse
 * for instance
 *
 * Once we have sorted, we cannot do a HashJoin or a CoGrouping
 */
trait SortedGrouped[K, +V]
  extends KeyedListLike[K, V, SortedGrouped]
  with WithReducers[SortedGrouped[K, V]]

/**
 * This is the state after we have done some reducing. It is
 * not possible to sort at this phase, but it is possible to
 * do a CoGrouping or a HashJoin.
 */
trait UnsortedGrouped[K, +V]
  extends KeyedListLike[K, V, UnsortedGrouped]
  with HashJoinable[K, V]
  with WithReducers[UnsortedGrouped[K, V]]

object Grouped {
  val ValuePosition: Int = 1 // The values are kept in this position in a Tuple
  val valueField: Fields = new Fields("value")
  val kvFields: Fields = new Fields("key", "value")

  def apply[K, V](pipe: TypedPipe[(K, V)])(implicit ordering: Ordering[K]): Grouped[K, V] =
    IdentityReduce(ordering, pipe, None)

  def keySorting[T](ord: Ordering[T]): Fields = sorting("key", ord)
  def valueSorting[T](implicit ord: Ordering[T]): Fields = sorting("value", ord)

  def sorting[T](key: String, ord: Ordering[T]): Fields = {
    val f = new Fields(key)
    f.setComparator(key, ord)
    f
  }
}

trait Sortable[+T, +Sorted[+_]] {
  def withSortOrdering[U >: T](so: Ordering[U]): Sorted[T]

  def sortBy[B: Ordering](fn: (T) => B): Sorted[T] =
    withSortOrdering(Ordering.by(fn))

  // Sorts the values for each key
  def sorted[B >: T](implicit ord: Ordering[B]): Sorted[T] =
    withSortOrdering(ord)

  def sortWith(lt: (T, T) => Boolean): Sorted[T] =
    withSortOrdering(Ordering.fromLessThan(lt))
}

// Represents something that when we call reverse changes type to R
trait Reversable[+R] {
  def reverse: R
}

/**
 * This is a class that models the logical portion of the reduce step.
 * details like where this occurs, the number of reducers, etc... are
 * left in the Grouped class
 */
sealed trait ReduceStep[K, V1] extends KeyedPipe[K] {
  /**
   * Note, this satisfies KeyedPipe.mapped: TypedPipe[(K, Any)]
   */
  def mapped: TypedPipe[(K, V1)]
  // make the pipe and group it, only here because it is common
  protected def groupOp[V2](gb: GroupBuilder => GroupBuilder): TypedPipe[(K, V2)] = {
    TypedPipeFactory({ (fd, mode) =>
      val reducedPipe = mapped
        .toPipe(Grouped.kvFields)(fd, mode, tup2Setter)
        .groupBy(Grouped.keySorting(keyOrdering))(gb)
      TypedPipe.from(reducedPipe, Grouped.kvFields)(fd, mode, tuple2Converter[K, V2])
    })
  }
}

case class IdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int])
  extends ReduceStep[K, V1]
  with Grouped[K, V1] {

  override def withSortOrdering[U >: V1](so: Ordering[U]): IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, so, reducers)

  override def withReducers(red: Int): IdentityReduce[K, V1] =
    copy(reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    UnsortedIdentityReduce(keyOrdering, mapped.filterKeys(fn), reducers)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) =
    IteratorMappedReduce(keyOrdering, mapped, fn, reducers)

  // It would be nice to return IdentityReduce here, but
  // the type constraints prevent it currently
  override def mapValues[V2](fn: V1 => V2) =
    UnsortedIdentityReduce(keyOrdering, mapped.mapValues(fn), reducers)

  // This is not correct in the type-system, but would be nice to encode
  //override def mapValues[V3](fn: V1 => V3) = IdentityReduce(keyOrdering, mapped.mapValues(fn), reducers)

  override def sum[U >: V1](implicit sg: Semigroup[U]) = {
    // there is no sort, mapValueStream or force to reducers:
    val upipe: TypedPipe[(K, U)] = mapped // use covariance to set the type
    UnsortedIdentityReduce(keyOrdering, upipe.sumByLocalKeys, reducers).sumLeft
  }

  override lazy val toTypedPipe = reducers match {
    case None => mapped // free case
    case Some(reds) =>
      // This is wierd, but it is sometimes used to force a partition
      groupOp { _.reducers(reds) }
  }

  /** This is just an identity that casts the result to V1 */
  override def joinFunction = CoGroupable.castingJoinFunction[V1]
}

case class UnsortedIdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int])
  extends ReduceStep[K, V1]
  with UnsortedGrouped[K, V1] {

  override def withReducers(red: Int): UnsortedIdentityReduce[K, V1] =
    copy(reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    UnsortedIdentityReduce(keyOrdering, mapped.filterKeys(fn), reducers)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) =
    IteratorMappedReduce(keyOrdering, mapped, fn, reducers)

  // It would be nice to return IdentityReduce here, but
  // the type constraints prevent it currently
  override def mapValues[V2](fn: V1 => V2) =
    UnsortedIdentityReduce(keyOrdering, mapped.mapValues(fn), reducers)

  override def sum[U >: V1](implicit sg: Semigroup[U]) = {
    // there is no sort, mapValueStream or force to reducers:
    val upipe: TypedPipe[(K, U)] = mapped // use covariance to set the type
    UnsortedIdentityReduce(keyOrdering, upipe.sumByLocalKeys, reducers).sumLeft
  }

  override lazy val toTypedPipe = reducers match {
    case None => mapped // free case
    case Some(reds) =>
      // This is wierd, but it is sometimes used to force a partition
      groupOp { _.reducers(reds) }
  }

  /** This is just an identity that casts the result to V1 */
  override def joinFunction = CoGroupable.castingJoinFunction[V1]
}

case class IdentityValueSortedReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[_ >: V1],
  override val reducers: Option[Int]) extends ReduceStep[K, V1]
  with SortedGrouped[K, V1]
  with Reversable[IdentityValueSortedReduce[K, V1]] {

  override def reverse: IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort.reverse, reducers)

  override def withReducers(red: Int): IdentityValueSortedReduce[K, V1] =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort, reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped.filterKeys(fn), valueSort, reducers)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) =
    ValueSortedReduce[K, V1, V3](keyOrdering, mapped, valueSort, fn, reducers)

  override lazy val toTypedPipe =
    groupOp {
      _.sortBy(Grouped.valueSorting(valueSort))
        .reducers(reducers.getOrElse(-1))
    }
}

case class ValueSortedReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[_ >: V1],
  reduceFn: (K, Iterator[V1]) => Iterator[V2],
  override val reducers: Option[Int])
  extends ReduceStep[K, V1] with SortedGrouped[K, V2] {

  override def withReducers(red: Int) =
    // copy infers loose types. :(
    ValueSortedReduce[K, V1, V2](
      keyOrdering, mapped, valueSort, reduceFn, Some(red))

  override def filterKeys(fn: K => Boolean) =
    // copy fails to get the types right, :/
    ValueSortedReduce[K, V1, V2](keyOrdering, mapped.filterKeys(fn), valueSort, reduceFn, reducers)

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    // don't make a closure
    val localRed = reduceFn
    val newReduce = { (k: K, iter: Iterator[V1]) => fn(k, localRed(k, iter)) }
    ValueSortedReduce[K, V1, V3](
      keyOrdering, mapped, valueSort, newReduce, reducers)
  }

  override lazy val toTypedPipe = {
    val vSort = Grouped.valueSorting(valueSort)

    groupOp {
      _.sortBy(vSort)
        .every(new cascading.pipe.Every(_, Grouped.valueField,
          new TypedBufferOp(reduceFn, Grouped.valueField), Fields.REPLACE))
        .reducers(reducers.getOrElse(-1))
    }
  }
}

case class IteratorMappedReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  reduceFn: (K, Iterator[V1]) => Iterator[V2],
  override val reducers: Option[Int])
  extends ReduceStep[K, V1] with UnsortedGrouped[K, V2] {

  override def withReducers(red: Int): IteratorMappedReduce[K, V1, V2] =
    copy(reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    copy(mapped = mapped.filterKeys(fn))

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    // don't make a closure
    val localRed = reduceFn
    val newReduce = { (k: K, iter: Iterator[V1]) => fn(k, localRed(k, iter)) }
    copy(reduceFn = newReduce)
  }

  override lazy val toTypedPipe =
    groupOp {
      _.every(new cascading.pipe.Every(_, Grouped.valueField,
        new TypedBufferOp(reduceFn, Grouped.valueField), Fields.REPLACE))
        .reducers(reducers.getOrElse(-1))
    }

  override def joinFunction = {
    // don't make a closure
    val localRed = reduceFn;
    { (k, iter, empties) =>
      assert(empties.isEmpty, "this join function should never be called with non-empty right-most")
      localRed(k, iter.map(_.getObject(Grouped.ValuePosition).asInstanceOf[V1]))
    }
  }
}

