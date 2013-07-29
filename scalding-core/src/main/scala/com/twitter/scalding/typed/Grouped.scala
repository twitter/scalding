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
  // Make a new Grouped from a pipe with two fields: 'key, 'value
  def fromKVPipe[K,V](pipe : Pipe, ordering : Ordering[K])
    (implicit conv : TupleConverter[V]) : Grouped[K,V] = {
    new Grouped[K,V](pipe, ordering, None, None, -1, false)
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
class Grouped[K,+T] private (private[scalding] val pipe : Pipe,
  val ordering : Ordering[K],
  streamMapFn : Option[(Iterator[CTuple]) => Iterator[T]],
  private[scalding] val valueSort : Option[(Fields,Boolean)],
  val reducers : Int = -1,
  val toReducers: Boolean = false)
  extends KeyedList[K,T] with Serializable {

  import Dsl._
  private[scalding] val groupKey = Grouped.sorting("key", ordering)

  protected def sortIfNeeded(gb : GroupBuilder) : GroupBuilder = {
    valueSort.map { fb =>
      val gbSorted = gb.sortBy(fb._1)
      if (fb._2) gbSorted.reverse else gbSorted
    }.getOrElse(gb)
  }
  def forceToReducers: Grouped[K,T] =
    new Grouped(pipe, ordering, streamMapFn, valueSort, reducers, true)
  def withSortOrdering[U >: T](so : Ordering[U]) : Grouped[K,T] = {
    // Set the sorting with unreversed
    assert(valueSort.isEmpty, "Can only call withSortOrdering once")
    assert(streamMapFn.isEmpty, "Cannot sort after a mapValueStream")
    val newValueSort = Some(Grouped.valueSorting(so)).map { f => (f,false) }
    new Grouped(pipe, ordering, None, newValueSort, reducers, toReducers)
  }
  def withReducers(red : Int) : Grouped[K,T] = {
    new Grouped(pipe, ordering, streamMapFn, valueSort, red, toReducers)
  }
  def sortBy[B](fn : (T) => B)(implicit ord : Ordering[B]) : Grouped[K,T] =
    withSortOrdering(Ordering.by(fn))

  // Sorts the values for each key
  def sorted[B >: T](implicit ord : Ordering[B]) : Grouped[K,T] = {
    // This cast is okay, because we are using the compare function
    // which is covariant, but the max/min functions are not, and that
    // breaks covariance.
    withSortOrdering(ord.asInstanceOf[Ordering[T]])
  }

  def sortWith(lt : (T,T) => Boolean) : Grouped[K,T] = {
    withSortOrdering(Ordering.fromLessThan(lt))
  }
  def reverse : Grouped[K,T] = {
    assert(streamMapFn.isEmpty, "Cannot reverse after mapValueStream")
    val newValueSort = valueSort.map { f => (f._1, !(f._2)) }
    new Grouped(pipe, ordering, None, newValueSort, reducers, toReducers)
  }

  protected def operate[T1](fn : GroupBuilder => GroupBuilder) : TypedPipe[(K,T1)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      val out = fn(sortIfNeeded(gb)).reducers(reducers)
      if(toReducers) out.forceToReducers else out
    }
    TypedPipe.from(reducedPipe, ('key, 'value))(tuple2Converter[K,T1])
  }
  // Here are the required KeyedList methods:
  override lazy val toTypedPipe : TypedPipe[(K,T)] = {
    if (streamMapFn.isEmpty && valueSort.isEmpty && (reducers == -1)) {
      // There was no reduce AND no mapValueStream, no need to groupBy:
      TypedPipe.from(pipe, ('key, 'value))(tuple2Converter[K,T])
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
  override def mapValues[V](fn : T => V) : Grouped[K,V] = {
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We have no sort defined yet, so we should operate on the pipe so we can sort by V after
      // if we need to:
      new Grouped(pipe.map('value -> 'value)(fn)(singleConverter[T], singleSetter[V]),
        ordering, None, None, reducers, toReducers)
    }
    else {
      // There is a sorting, which invalidates map-side optimizations,
      // so we might as well use mapValueStream
      mapValueStream { iter => iter.map { fn } }
    }
  }
  // If there is no ordering, this operation is pushed map-side
  override def reduce[U >: T](fn : (U,U) => U) : TypedPipe[(K,U)] = {
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We can optimize mapside:
      val msr = new MapsideReduce(Semigroup.from(fn), 'key, 'value, None)(singleConverter[U], singleSetter[U])
      val mapSideReduced = pipe.eachTo(('key, 'value) -> ('key, 'value)) { _ => msr }
      // Now force to reduce-side for the rest, use groupKey to get the correct ordering
      val reducedPipe = mapSideReduced.groupBy(groupKey) {
        _.reduce('value -> 'value)(fn)(singleSetter[U], singleConverter[U])
          .reducers(reducers)
          .forceToReducers
      }
      TypedPipe.from(reducedPipe, ('key, 'value))(tuple2Converter[K,U])
    }
    else {
      // Just fall back to the mapValueStream based implementation:
      reduceLeft(fn)
    }
  }
  private[scalding] lazy val streamMapping : (Iterator[CTuple]) => Iterator[T] = {
    streamMapFn.getOrElse {
      // Set up the initial stream mapping:
      {(ti : Iterator[CTuple]) => ti.map { _.getObject(0).asInstanceOf[T] }}
    }
  }
  override def mapValueStream[V](nmf : Iterator[T] => Iterator[V]) : Grouped[K,V] = {
    val newStreamMapFn = Some(streamMapping.andThen(nmf))
    new Grouped[K,V](pipe, ordering, newStreamMapFn, valueSort, reducers, toReducers)
  }
  // SMALLER PIPE ALWAYS ON THE RIGHT!!!!!!!
  def cogroup[W,R](smaller: Grouped[K,W])(joiner: (K, Iterator[T], Iterable[W]) => Iterator[R])
    : KeyedList[K,R] = new CoGrouped2[K,T,W,R](this, smaller, joiner)

  def join[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.inner2)
  def leftJoin[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.left2)
  def rightJoin[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.right2)
  def outerJoin[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.outer2)

  /** WARNING This behaves semantically very differently than cogroup.
   * this is because we handle (K,T) tuples on the left as we see them.
   * the iterator on the right is over all elements with a matching key K, and it may be empty
   * if there are no values for this key K.
   * (because you haven't actually cogrouped, but only read the right hand side into a hashtable)
   */
  def hashCogroup[W,R](smaller: Grouped[K,W])(joiner: (K, T, Iterable[W]) => Iterator[R])
    : TypedPipe[(K,R)] = (new HashCoGrouped2[K,T,W,R](this, smaller, joiner)).toTypedPipe

  def hashJoin[W](smaller : Grouped[K,W]) : TypedPipe[(K,(T,W))] =
    hashCogroup(smaller)(Joiner.hashInner2)

  def hashLeftJoin[W](smaller : Grouped[K,W]) : TypedPipe[(K,(T,Option[W]))] =
    hashCogroup(smaller)(Joiner.hashLeft2)

  // TODO: implement blockJoin
}
