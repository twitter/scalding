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

object Grouped {
  val valueField: Fields = new Fields("value")
  val kvFields: Fields = new Fields("key", "value")
  // Make a new Grouped from a pipe with two fields: 'key, 'value
  def apply[K,V](pipe: TypedPipe[(K,V)])(implicit ordering: Ordering[K]): Grouped[K,V] =
    new Grouped[K,V](IdentityReduce(pipe), ordering, None, -1, false)

  def valueSorting[T](implicit ord : Ordering[T]) : Fields = sorting("value", ord)

  def sorting[T](key : String, ord : Ordering[T]) : Fields = {
    val f = new Fields(key)
    f.setComparator(key, ord)
    f
  }
}

/**
 * This is a class that models the logical portion of the reduce step.
 * details like where this occurs, the number of reducers, etc... are
 * left in the Grouped class
 */
sealed trait ReduceStep[+K, V1, +V2] extends java.io.Serializable {
  def mapped: TypedPipe[(K, V1)]
  def reduceFn: (Iterator[V1] => Iterator[V2])
  def andThen[V3](fn: Iterator[V2] => Iterator[V3]): ReduceStep[K, V1, V3]
  def mapValues[V3](fn: V2 => V3): ReduceStep[K, _, V3]
  def operate(gb: GroupBuilder): GroupBuilder
  def streamMapping: Iterator[CTuple] => Iterator[V2]
}

case class IdentityReduce[K, V1](
  override val mapped: TypedPipe[(K, V1)])
    extends ReduceStep[K, V1, V1] {

  def reduceFn = identity
  def andThen[V3](fn: Iterator[V1] => Iterator[V3]): ReduceStep[K, V1, V3] =
    ReduceOp(mapped, fn)
  def mapValues[V3](fn: V1 => V3): ReduceStep[K, _, V3] =
    IdentityReduce(mapped.mapValues(fn))

  def operate(gb: GroupBuilder) = gb
  def streamMapping = { iter => iter.map(_.getObject(0).asInstanceOf[V1]) }
}

case class ReduceOp[K, V1, V2](override val mapped: TypedPipe[(K, V1)],
  override val reduceFn: Iterator[V1] => Iterator[V2]) extends ReduceStep[K, V1, V2] {

  def andThen[V3](fn: Iterator[V2] => Iterator[V3]): ReduceStep[K, V1, V3] =
    ReduceOp(mapped, reduceFn.andThen(fn))

  def mapValues[V3](fn: V2 => V3): ReduceStep[K, _, V3] = {
    // don't make a closure
    val localRed = reduceFn
    ReduceOp[K,V1,V3](mapped, localRed(_).map(fn))
  }

  def operate(gb: GroupBuilder) =
    gb.mapStream[V1, V2](Grouped.valueField -> Grouped.valueField)(reduceFn)

  def streamMapping = {
    // don't make a closure
    val localRed = reduceFn;
    { iter => localRed(iter.map(_.getObject(0).asInstanceOf[V1])) }
  }
}

/** Represents a grouping which is the transition from map to reduce phase in hadoop.
 * Grouping is on a key of type K by ordering Ordering[K].
 */
class Grouped[+K,+T] private (@transient val reduceStep: ReduceStep[K, _, T],
  ord: Ordering[K],
  private[scalding] val valueSort : Option[(Fields,Boolean)],
  val reducers : Int = -1,
  val toReducers: Boolean = false)
  extends KeyedList[K,T] with Serializable {

  type This[+K, +T] = Grouped[K, T]

  import Dsl._
  /* The following three things are used by CoGroup2 to handle joins */
  private[scalding] val groupKey = Grouped.sorting("key", ordering)
  private[scalding] def pipe: Pipe = reduceStep.mapped.toPipe(Grouped.kvFields)
  def streamMapping: Iterator[CTuple] => Iterator[T] = reduceStep.streamMapping

  def ordering: Ordering[_ <: K] = ord

  // We have to pass in the ordering due to variance. Cleaner solutions welcome
  protected def changeReduce[K1,V](rs: ReduceStep[K1, _, V], no: Ordering[K1]): Grouped[K1, V] =
    new Grouped(rs, no, valueSort, reducers, toReducers)

  protected def copy(
    valueSort: Option[(Fields,Boolean)] = valueSort,
    reducers: Int = reducers,
    toReducers: Boolean = toReducers): Grouped[K, T] =
      new Grouped(reduceStep, ord, valueSort, reducers, toReducers)

  def forceToReducers: Grouped[K,T] = copy(toReducers = true)

  private def reduceIsIdentity = reduceStep match {
    case IdentityReduce(_) => true
    case _ => false
  }

  def withSortOrdering[U >: T](so : Ordering[U]) : Grouped[K,T] = {
    // Set the sorting with unreversed
    assert(valueSort.isEmpty, "Can only call withSortOrdering once")
    assert(reduceIsIdentity, "Cannot sort after a mapValueStream")
    val newValueSort = Some(Grouped.valueSorting(so)).map { f => (f,false) }
    copy(valueSort = newValueSort)
  }

  def withReducers(red : Int) : Grouped[K,T] = copy(reducers = red)

  def sortBy[B](fn : (T) => B)(implicit ord : Ordering[B]) : Grouped[K,T] =
    withSortOrdering(Ordering.by(fn))

  // Sorts the values for each key
  def sorted[B >: T](implicit ord : Ordering[B]) : Grouped[K,T] =
    // This cast is okay, because we are using the compare function
    // which is covariant, but the max/min functions are not, and that
    // breaks covariance.
    withSortOrdering(ord.asInstanceOf[Ordering[T]])

  def sortWith(lt : (T,T) => Boolean) : Grouped[K,T] =
    withSortOrdering(Ordering.fromLessThan(lt))

  def reverse : Grouped[K,T] = {
    assert(reduceIsIdentity, "Cannot reverse after a mapValueStream")
    val newValueSort = valueSort.map { f => (f._1, !(f._2)) }
    copy(valueSort = newValueSort)
  }

  // Here are the required KeyedList methods:
  override lazy val toTypedPipe : TypedPipe[(K,T)] =
    (reduceStep, valueSort, reducers) match {
      case (IdentityReduce(idPipe), None, -1) =>
        // There was no reduce AND no mapValueStream, no reducer change =>
        // no need to groupBy:
        idPipe
      case _ =>
        // helper function we call exactly once below
        def sortIfNeeded(gb: GroupBuilder): GroupBuilder =
          valueSort.map { fb =>
            val gbSorted = gb.sortBy(fb._1)
            if (fb._2) gbSorted.reverse else gbSorted
          }.getOrElse(gb)

        val reducedPipe = pipe.groupBy(groupKey) { gb =>
          val preop = sortIfNeeded(gb).reducers(reducers)
          val out = reduceStep.operate(preop)
          if(toReducers) out.forceToReducers else out
        }
        TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,T])
    }

  override def mapValues[V](fn : T => V): Grouped[K,V] =
    if(valueSort.isEmpty) {
      // We have no sort defined yet,
      // so we should operate on the pipe so we can sort by V after
      // if we need to:
      changeReduce(reduceStep.mapValues(fn), ord)
    }
    else {
      // There is a sorting, so, we have to map after the sort
      mapValueStream(_.map(fn))
    }

  // If there is no ordering, this operation is pushed map-side
  override def sum[U >: T](implicit sg: Semigroup[U]): TypedPipe[(K,U)] =
    (valueSort, reduceStep, toReducers) match {
      case (None, IdentityReduce(pipe), false) =>
        // there is no sort, mapValueStream or force to reducers:
        val upipe: TypedPipe[(K, U)] = pipe // use covariance to set the type
        changeReduce(IdentityReduce(upipe.sumByLocalKeys), ord).sumLeft
      case _ =>
        // Just fall back to the mapValueStream based implementation:
        sumLeft[U]
    }

  override def mapValueStream[V](nmf : Iterator[T] => Iterator[V]) : Grouped[K,V] =
    changeReduce(reduceStep.andThen(nmf), ord)

  // SMALLER PIPE ALWAYS ON THE RIGHT!!!!!!!
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

  /** WARNING This behaves semantically very differently than cogroup.
   * this is because we handle (K,T) tuples on the left as we see them.
   * the iterator on the right is over all elements with a matching key K, and it may be empty
   * if there are no values for this key K.
   * (because you haven't actually cogrouped, but only read the right hand side into a hashtable)
   */
  def hashCogroup[K1>:K,W,R](smaller: Grouped[K1,W])(joiner: (K1, T, Iterable[W]) => Iterator[R])
    : TypedPipe[(K1,R)] = (new HashCoGrouped2[K1,T,W,R](this, smaller, joiner)).toTypedPipe

  def hashJoin[K1>:K,W](smaller : Grouped[K1,W]) : TypedPipe[(K1,(T,W))] =
    hashCogroup(smaller)(Joiner.hashInner2)

  def hashLeftJoin[K1>:K,W](smaller : Grouped[K1,W]) : TypedPipe[(K1,(T,Option[W]))] =
    hashCogroup(smaller)(Joiner.hashLeft2)

  // TODO: implement blockJoin
}
