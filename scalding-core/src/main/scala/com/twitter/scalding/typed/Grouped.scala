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
  val kvFields: Fields = new Fields("key", "value")
  // Make a new Grouped from a pipe with two fields: 'key, 'value
  def apply[K,V](pipe: TypedPipe[(K,V)])(implicit ordering: Ordering[K]): Grouped[K,V] = {
    val gpipe = pipe.toPipe(kvFields)(TupleSetter.tup2Setter[(K,V)])
    new Grouped[K,V](gpipe, ordering, None, None, -1, false)
  }
  def valueSorting[T](implicit ord : Ordering[T]) : Fields = sorting("value", ord)

  def sorting[T](key : String, ord : Ordering[T]) : Fields = {
    val f = new Fields(key)
    f.setComparator(key, ord)
    f
  }
}
/** Represents a grouping which is the transition from map to reduce phase in hadoop.
 * Grouping is on a key of type K by ordering Ordering[K].
 */
class Grouped[+K,+T] private (@transient val pipe : Pipe,
  ord: Ordering[K],
  streamMapFn : Option[(Iterator[CTuple]) => Iterator[T]],
  private[scalding] val valueSort : Option[(Fields,Boolean)],
  val reducers : Int = -1,
  val toReducers: Boolean = false)
  extends KeyedList[K,T] with Serializable {

  type This[+K, +T] = Grouped[K, T]

  import Dsl._
  private[scalding] val groupKey = Grouped.sorting("key", ordering)

  def ordering: Ordering[_ <: K] = ord

  protected def copy[V](
    streamMapFn: Option[(Iterator[CTuple]) => Iterator[V]],
    pipe: Pipe = pipe,
    valueSort: Option[(Fields,Boolean)] = valueSort,
    reducers: Int = reducers,
    toReducers: Boolean = toReducers): Grouped[K, V] =
      new Grouped(pipe, ord, streamMapFn, valueSort, reducers, toReducers)

  protected def sortIfNeeded(gb : GroupBuilder) : GroupBuilder = {
    valueSort.map { fb =>
      val gbSorted = gb.sortBy(fb._1)
      if (fb._2) gbSorted.reverse else gbSorted
    }.getOrElse(gb)
  }
  def forceToReducers: Grouped[K,T] = copy(streamMapFn, toReducers = true)

  def withSortOrdering[U >: T](so : Ordering[U]) : Grouped[K,T] = {
    // Set the sorting with unreversed
    assert(valueSort.isEmpty, "Can only call withSortOrdering once")
    assert(streamMapFn.isEmpty, "Cannot sort after a mapValueStream")
    val newValueSort = Some(Grouped.valueSorting(so)).map { f => (f,false) }
    copy[T](None, valueSort = newValueSort)
  }

  def withReducers(red : Int) : Grouped[K,T] = copy(streamMapFn, reducers = red)

  def sortBy[B](fn : (T) => B)(implicit ord : Ordering[B]) : Grouped[K,T] =
    withSortOrdering(Ordering.by(fn))

  // Sorts the values for each key
  def sorted[B >: T](implicit ord : Ordering[B]) : Grouped[K,T] = {
    // This cast is okay, because we are using the compare function
    // which is covariant, but the max/min functions are not, and that
    // breaks covariance.
    withSortOrdering(ord.asInstanceOf[Ordering[T]])
  }

  def sortWith(lt : (T,T) => Boolean) : Grouped[K,T] =
    withSortOrdering(Ordering.fromLessThan(lt))

  def reverse : Grouped[K,T] = {
    assert(streamMapFn.isEmpty, "Cannot reverse after mapValueStream")
    val newValueSort = valueSort.map { f => (f._1, !(f._2)) }
    copy[T](None, valueSort = newValueSort)
  }

  protected def operate[T1](fn : GroupBuilder => GroupBuilder) : TypedPipe[(K,T1)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      val out = fn(sortIfNeeded(gb)).reducers(reducers)
      if(toReducers) out.forceToReducers else out
    }
    TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,T1])
  }
  // Here are the required KeyedList methods:
  override lazy val toTypedPipe : TypedPipe[(K,T)] = {
    if (streamMapFn.isEmpty && valueSort.isEmpty && (reducers == -1)) {
      // There was no reduce AND no mapValueStream, no need to groupBy:
      TypedPipe.from(pipe, Grouped.kvFields)(tuple2Converter[K,T])
    }
    else {
      //Actually execute the mapValueStream:
      streamMapFn.map { fn =>
        operate[T] {
          _.mapStream[CTuple,T]('value -> 'value)(fn)(CTupleConverter, singleSetter[T])
        }
      }.getOrElse {
        // This case happens when someone does .groupAll.sortBy { }.write
        // so there is no operation, they are just doing a sorted write
        operate[T] { identity _ }
      }
    }
  }
  override def mapValues[V](fn : T => V): Grouped[K,V] =
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We have no sort defined yet, so we should operate on the pipe so we can sort by V after
      // if we need to:
      copy[V](None, pipe = pipe.map('value -> 'value)(fn)(singleConverter[T], singleSetter[V]))
    }
    else {
      // There is a sorting, which invalidates map-side optimizations,
      // so we might as well use mapValueStream
      mapValueStream { iter => iter.map { fn } }
    }

  // If there is no ordering, this operation is pushed map-side
  override def sum[U >: T](implicit sg: Semigroup[U]): TypedPipe[(K,U)] =
    if(valueSort.isEmpty && streamMapFn.isEmpty && (!toReducers)) {
      // We can optimize mapside:
      val msr = new MapsideReduce(sg, 'key, 'value, None)(singleConverter[U], singleSetter[U])

      val mapSideReduced = pipe.eachTo(Grouped.kvFields -> Grouped.kvFields) { _ => msr }
      // Now force to reduce-side for the rest:
      copy[U](None, pipe = mapSideReduced).sumLeft
    }
    else {
      // Just fall back to the mapValueStream based implementation:
      sumLeft[U]
    }

  private[scalding] lazy val streamMapping : (Iterator[CTuple]) => Iterator[T] =
    streamMapFn.getOrElse {
      // Set up the initial stream mapping:
      {(ti : Iterator[CTuple]) => ti.map { _.getObject(0).asInstanceOf[T] }}
    }

  override def mapValueStream[V](nmf : Iterator[T] => Iterator[V]) : Grouped[K,V] = {
    val newStreamMapFn = Some(streamMapping.andThen(nmf))
    copy(newStreamMapFn)
  }
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
