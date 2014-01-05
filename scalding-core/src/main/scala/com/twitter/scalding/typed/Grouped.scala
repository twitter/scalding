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

import com.twitter.algebird.{Semigroup, Ring, Aggregator}

import com.twitter.scalding.TupleConverter.{singleConverter, tuple2Converter, CTupleConverter, TupleEntryConverter}
import com.twitter.scalding.TupleSetter.{singleSetter, tup2Setter}

import com.twitter.scalding._

import cascading.pipe.Pipe
import cascading.tuple.{Fields, Tuple => CTuple, TupleEntry}

import Dsl._

object Grouped {
  val ValuePosition: Int = 1 // The values are kept in this position in a Tuple
  val valueField: Fields = new Fields("value")
  val kvFields: Fields = new Fields("key", "value")
  // Make a new Grouped from a pipe with two fields: 'key, 'value
  def apply[K,V](pipe: TypedPipe[(K,V)])(implicit ordering: Ordering[K]): Grouped[K,V] =
    new Grouped[K,V](IdentityReduce(ordering, pipe), -1, false)

  def keySorting[T](ord : Ordering[T]): Fields = sorting("key", ord)
  def valueSorting[T](implicit ord : Ordering[T]) : Fields = sorting("value", ord)

  def sorting[T](key : String, ord : Ordering[T]) : Fields = {
    val f = new Fields(key)
    f.setComparator(key, ord)
    f
  }

  def emptyStreamMapping[V]: Iterator[CTuple] => Iterator[V] =
    { iter => iter.map(_.getObject(ValuePosition).asInstanceOf[V]) }
}

/**
 * This is a class that models the logical portion of the reduce step.
 * details like where this occurs, the number of reducers, etc... are
 * left in the Grouped class
 */
sealed trait ReduceStep[+K, V1, +V2] extends java.io.Serializable {
  def keyOrdering: Ordering[_ <: K]
  def valueOrdering: Option[Ordering[_ >: V1]]

  def mapped: TypedPipe[(K, V1)]

  def mappedPipe: Pipe = mapped.toPipe(Grouped.kvFields)

  def reduceFn: (Iterator[V1] => Iterator[V2])
  def andThen[V3](fn: Iterator[V2] => Iterator[V3]): ReduceStep[K, V1, V3]
  def mapValues[V3](fn: V2 => V3): ReduceStep[K, _, V3]
  def toTypedPipe(reducers: Int, forceToReducers: Boolean): TypedPipe[(K, V2)]
  def streamMapping: Iterator[CTuple] => Iterator[V2]
}

case class IdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)])
    extends ReduceStep[K, V1, V1] {

  def valueOrdering = None

  def withSort(ord: Ordering[_ >: V1]): IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, ord)

  def reduceFn = identity
  def andThen[V3](fn: Iterator[V1] => Iterator[V3]): ReduceStep[K, V1, V3] =
    IteratorMappedReduce(Right(this), fn)

  def mapValues[V3](fn: V1 => V3): ReduceStep[K, _, V3] =
    IdentityReduce(keyOrdering, mapped.mapValues(fn))

  def toTypedPipe(reducers: Int, forceToReducers: Boolean) =
    if(reducers == -1 && (!forceToReducers)) mapped // free case
    else {
      // This is wierd, but it is sometimes used to force a partition
      val reducedPipe = mappedPipe.groupBy(Grouped.keySorting(keyOrdering)) {
            _.reducers(reducers)
        }
      TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,V1])
    }

  def streamMapping = Grouped.emptyStreamMapping[V1]
}

case class IdentityValueSortedReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[_ >: V1]
  ) extends ReduceStep[K, V1, V1] {

  def reverseSort: IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort.reverse)

  def valueOrdering = Some(valueSort)

  def reduceFn = identity
  def andThen[V3](fn: Iterator[V1] => Iterator[V3]): ReduceStep[K, V1, V3] =
    IteratorMappedReduce(Left(this), fn)

  // Once we have sorted, we have to create a IteratorMappedReduce to map.
  def mapValues[V3](fn: V1 => V3): ReduceStep[K, _, V3] =
    IteratorMappedReduce[K,V1,V3](Left(this), _.map(fn))

  def toTypedPipe(reducers: Int, forceToReducers: Boolean) = {
    val reducedPipe = mappedPipe.groupBy(Grouped.keySorting(keyOrdering)) {
        _.sortBy(Grouped.valueSorting(valueSort))
          .reducers(reducers)
      }
    TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,V1])
  }

  def streamMapping = Grouped.emptyStreamMapping[V1]
}

case class IteratorMappedReduce[K, V1, V2](
  prepared: Either[IdentityValueSortedReduce[K, V1], IdentityReduce[K, V1]],
  override val reduceFn: Iterator[V1] => Iterator[V2]) extends ReduceStep[K, V1, V2] {

  def mapped = prepared.fold(_.mapped, _.mapped)

  def keyOrdering = prepared.fold(_.keyOrdering, _.keyOrdering)
  def valueOrdering = prepared.fold(_.valueOrdering: Option[Ordering[_ >: V1]], _ => None)

  def andThen[V3](fn: Iterator[V2] => Iterator[V3]): ReduceStep[K, V1, V3] =
    IteratorMappedReduce(prepared, reduceFn.andThen(fn))

  def mapValues[V3](fn: V2 => V3): ReduceStep[K, _, V3] = {
    // don't make a closure
    val localRed = reduceFn
    IteratorMappedReduce[K,V1,V3](prepared, localRed(_).map(fn))
  }

  def toTypedPipe(reducers: Int, forceToReducers: Boolean) = {
    val optVSort = prepared.fold(
      {ivsr => Some(Grouped.valueSorting(ivsr.valueSort))},
      _ => None)

    val reducedPipe = mappedPipe.groupBy(Grouped.keySorting(keyOrdering)) { gb =>
        optVSort.map { s => gb.sortBy(s) }
          .getOrElse(gb)
          .mapStream[V1, V2](Grouped.valueField -> Grouped.valueField)(reduceFn)
          .reducers(reducers)
      }
    TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,V2])
  }

  def streamMapping = {
    // don't make a closure
    val localRed = reduceFn;
    { iter => localRed(iter.map(_.getObject(Grouped.ValuePosition).asInstanceOf[V1])) }
  }
}

/** Represents a grouping which is the transition from map to reduce phase in hadoop.
 * Grouping is on a key of type K by ordering Ordering[K].
 */
class Grouped[+K,+T] private (@transient val reduceStep: ReduceStep[K, _, T],
  val reducers : Int = -1,
  val toReducers: Boolean = false)
  extends KeyedListLike[K,T,Grouped] with Serializable {

  // We have to pass in the ordering due to variance. Cleaner solutions welcome
  protected def changeReduce[K1,V](rs: ReduceStep[K1, _, V]): Grouped[K1, V] =
    new Grouped(rs, reducers, toReducers)

  protected def copy(
    reducers: Int = reducers,
    toReducers: Boolean = toReducers): Grouped[K, T] =
      new Grouped(reduceStep, reducers, toReducers)

  def forceToReducers: Grouped[K,T] = copy(toReducers = true)

  def withSortOrdering[U >: T](so: Ordering[U]): Grouped[K,T] =
    reduceStep match {
      case id@IdentityReduce(_, _) => changeReduce(id.withSort(so))
      case IdentityValueSortedReduce(_, _, _) =>
        sys.error("Can only call withSortOrdering once")
      case IteratorMappedReduce(_, _) =>
        sys.error("Cannot sort after a mapValueStream")
    }

  def withReducers(red: Int): Grouped[K,T] = copy(reducers = red)

  def sortBy[B:Ordering](fn : (T) => B): Grouped[K,T] =
    withSortOrdering(Ordering.by(fn))

  // Sorts the values for each key
  def sorted[B >: T](implicit ord : Ordering[B]): Grouped[K,T] =
    // This cast is okay, because we are using the compare function
    // which is covariant, but the max/min functions are not, and that
    // breaks covariance.
    withSortOrdering(ord.asInstanceOf[Ordering[T]])

  def sortWith(lt : (T,T) => Boolean): Grouped[K,T] =
    withSortOrdering(Ordering.fromLessThan(lt))

  def reverse: Grouped[K,T] = reduceStep match {
    case ivsr@IdentityValueSortedReduce(_, _, _) => changeReduce(ivsr.reverseSort)
    case IdentityReduce(_, _) => sys.error("Cannot reverse an unsorted reduce")
    case IteratorMappedReduce(_, _) => sys.error("Cannot reverse after reducing a stream")
  }

  // Here are the required KeyedList methods:
  override lazy val toTypedPipe : TypedPipe[(K,T)] =
    reduceStep.toTypedPipe(reducers, toReducers)

  override def mapValues[V](fn : T => V): Grouped[K,V] =
    changeReduce(reduceStep.mapValues(fn))

  // If there is no ordering, this operation is pushed map-side
  override def sum[U >: T](implicit sg: Semigroup[U]): Grouped[K,U] =
    (reduceStep, toReducers) match {
      case (IdentityReduce(ord, pipe), false) =>
        // there is no sort, mapValueStream or force to reducers:
        val upipe: TypedPipe[(K, U)] = pipe // use covariance to set the type
        changeReduce(IdentityReduce(ord, upipe.sumByLocalKeys)).sumLeft
      case _ =>
        // Just fall back to the mapValueStream based implementation:
        sumLeft[U]
    }

  override def mapValueStream[V](nmf : Iterator[T] => Iterator[V]) : Grouped[K,V] =
    changeReduce(reduceStep.andThen(nmf))

  /**
   * Smaller is about average values/key not total size (that does not matter, but is
   * clearly related).
   *
   * Note that from the type signature we see that the right side is iterated (or may be)
   * over and over, but the left side is not. That means that you want the side with
   * fewer values per key on the right. If both sides are similar, no need to worry.
   * If one side is a one-to-one mapping, that should be the "smaller" side.
   */
  def cogroup[K1>:K,W,R](smaller: Grouped[K1,W])(joiner: (K1, Iterator[T], Iterable[W]) => Iterator[R])
    : KeyedList[K1,R] = new CoGrouped2[K1,T,W,R](this, smaller, joiner)

  def join[K1>:K,W](smaller : Grouped[K1,W]) =
    cogroup[K1,W,(T,W)](smaller)(Joiner.inner2)
  def leftJoin[K1>:K,W](smaller : Grouped[K1,W]) =
    cogroup[K1,W,(T,Option[W])](smaller)(Joiner.left2)
  def rightJoin[K1>:K,W](smaller : Grouped[K1,W]) =
    cogroup[K1,W,(Option[T],W)](smaller)(Joiner.right2)
  def outerJoin[K1>:K,W](smaller : Grouped[K1,W]) =
    cogroup[K1,W,(Option[T],Option[W])](smaller)(Joiner.outer2)


  // TODO: implement blockJoin
}
