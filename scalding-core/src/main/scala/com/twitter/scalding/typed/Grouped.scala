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

import cascading.pipe.{HashJoin, Pipe}
import cascading.tuple.{Fields, Tuple => CTuple, TupleEntry}

import Dsl._

object Grouped {
  val ValuePosition: Int = 1 // The values are kept in this position in a Tuple
  val valueField: Fields = new Fields("value")
  val kvFields: Fields = new Fields("key", "value")

  def apply[K,V](pipe: TypedPipe[(K,V)])(implicit ordering: Ordering[K]): Grouped[K,V] =
    IdentityReduce(ordering, pipe, None)

  def keySorting[T](ord : Ordering[T]): Fields = sorting("key", ord)
  def valueSorting[T](implicit ord : Ordering[T]) : Fields = sorting("value", ord)

  def sorting[T](key : String, ord : Ordering[T]) : Fields = {
    val f = new Fields(key)
    f.setComparator(key, ord)
    f
  }

  def identityCastingJoin[K,V]: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[V] =
    { (k, iter, empties) =>
      assert(empties.isEmpty, "this join function should never be called with non-empty right-most")
      iter.map(_.getObject(ValuePosition).asInstanceOf[V])
    }
}

/** Represents a grouping which is the transition from map to reduce phase in hadoop.
 * Basically, this is a KeyedList that can be sorted and cogrouped.
 *
 * Grouping is on a key of type K by ordering Ordering[K].
 */
trait Grouped[K,+T] extends CoGroupable[K, T]
  with KeyedListLike[K,T,Grouped]
  with WithReducers[Grouped[K,T]]
  with Serializable {

  /** This can only be called once and before and mapValueStream/mapGroup/sum/reduce
   */
  def withSortOrdering[U >: T](so: Ordering[U]): Grouped[K,T]
  def reverse: Grouped[K,T]

  def keyOrdering: Ordering[K]
  ///////////
  // Defaults for the rest are in terms of the trait methods, or the above abstract methods
  ///////////

  /** This is just short hand for mapValueStream(identity), it makes sure the
   * planner sees that you want to force a shuffle. For expert tuning
   */
  def forceToReducers: Grouped[K,T] =
    mapValueStream(identity)

  /** This fully replicates this entire Grouped to the argument: mapside.
   * This means that we never see the case where the key is absent in the pipe. This
   * means implementing a right-join (from the pipe) is impossible.
   * Note, there is no reduce-phase in this operation.
   * The next issue is that obviously, unlike a cogroup, for a fixed key, each joiner will
   * NOT See all the tuples with those keys. This is because the keys on the left are
   * distributed across many machines
   * See hashjoin:
   * http://docs.cascading.org/cascading/2.0/javadoc/cascading/pipe/HashJoin.html
   */
  def hashCogroupOn[V,R](mapside: TypedPipe[(K, V)])(joiner: (K, V, Iterable[T]) => Iterator[R]): TypedPipe[(K,R)] = {
    // Note, the Ordering must have that compare(x,y)== 0 being consistent with hashCode and .equals to
    // otherwise, there may be funky issues with cascading
    val newPipe = new HashJoin(RichPipe.assignName(mapside.toPipe(('key, 'value))),
      RichFields(StringField("key")(keyOrdering, None)),
      mappedPipe(('key1, 'value1)),
      RichFields(StringField("key1")(keyOrdering, None)),
      //new HashJoiner[K,V,T,R](joinFunction, joiner))
      new HashJoiner(joinFunction, joiner))

    //Construct the new TypedPipe
    TypedPipe.from[(K,R)](newPipe.project('key,'value), ('key, 'value))
  }

  /**
   * produce the (key, value) Pipe that should be fed into and GroupBy
   * CoGroup, or HashJoin.
   */
  protected def mappedPipe(kvFields: Fields): Pipe

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
}

/**
 * This is a class that models the logical portion of the reduce step.
 * details like where this occurs, the number of reducers, etc... are
 * left in the Grouped class
 */
sealed trait ReduceStep[K, V1, +V2] extends Grouped[K, V2] with java.io.Serializable {
  def valueOrdering: Option[Ordering[_ >: V1]]
  def mapped: TypedPipe[(K, V1)]

  // When cogrouping, this is the only input
  def inputs = List(mapped)

  protected def mappedPipe(kvf: Fields): Pipe =
    mapped.toPipe(kvf)

  // make the pipe and group it, only here because it is common
  protected def groupOp(gb: GroupBuilder => GroupBuilder): Pipe =
    mappedPipe(Grouped.kvFields).groupBy(Grouped.keySorting(keyOrdering))(gb)
}

case class IdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int])
    extends ReduceStep[K, V1, V1] {

  override def withSortOrdering[U >: V1](so: Ordering[U]): IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, so, reducers)

  override def reverse =
    sys.error("Cannot reverse until after a value sorting is applied: " + toString)

  override def withReducers(red: Int): IdentityReduce[K, V1] =
    copy(reducers = Some(red))

  def valueOrdering = None

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]): ReduceStep[K, V1, V3] =
    IteratorMappedReduce(Right(this), fn, reducers)

  override def mapValues[V3](fn: V1 => V3): ReduceStep[K, _, V3] =
    IdentityReduce(keyOrdering, mapped.mapValues(fn), reducers)

  override def sum[U >: V1](implicit sg: Semigroup[U]) = {
    // there is no sort, mapValueStream or force to reducers:
    val upipe: TypedPipe[(K, U)] = mapped // use covariance to set the type
    IdentityReduce(keyOrdering, upipe.sumByLocalKeys, reducers).sumLeft
  }

  override lazy val toTypedPipe = reducers match {
    case None => mapped // free case
    case Some(reds) =>
      // This is wierd, but it is sometimes used to force a partition
      val reducedPipe = groupOp { _.reducers(reds) }
      TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,V1])
    }

  override def joinFunction = Grouped.identityCastingJoin
}

case class IdentityValueSortedReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[_ >: V1],
  override val reducers: Option[Int]
  ) extends ReduceStep[K, V1, V1] {

  override def withSortOrdering[U >: V1](so: Ordering[U]) =
    sys.error("Value sort has already been applied: " + toString)

  override def reverse: IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort.reverse, reducers)

  override def withReducers(red: Int): IdentityValueSortedReduce[K, V1] =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort, reducers = Some(red))

  def valueOrdering = Some(valueSort)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]): ReduceStep[K, V1, V3] =
    IteratorMappedReduce(Left(this), fn, reducers)

  // Once we have sorted, we have to create a IteratorMappedReduce to map.
  override def mapValues[V3](fn: V1 => V3): ReduceStep[K, _, V3] =
    IteratorMappedReduce[K,V1,V3](Left(this),
      { (k: K, iter: Iterator[V1]) => iter.map(fn) },
      reducers)

  override lazy val toTypedPipe = {
    val reducedPipe = groupOp {
        _.sortBy(Grouped.valueSorting(valueSort))
          .reducers(reducers.getOrElse(-1))
      }
    TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,V1])
  }

  override def joinFunction =
    sys.error("We do not support joining after value sorting. Try .toList.sorted in memory for now")
}

case class IteratorMappedReduce[K, V1, V2](
  prepared: Either[IdentityValueSortedReduce[K, V1], IdentityReduce[K, V1]],
  reduceFn: (K, Iterator[V1]) => Iterator[V2],
  override val reducers: Option[Int]) extends ReduceStep[K, V1, V2] {

  def withSortOrdering[U >: V2](so: Ordering[U]) =
    sys.error("Cannot change the value sort after we have set up a reduce mapping")

  def reverse =
    sys.error("Cannot reverse the value sort after we have set up a reduce mapping")

  override def withReducers(red: Int): IteratorMappedReduce[K, V1, V2] =
    copy(reducers = Some(red))

  def mapped = prepared.fold(_.mapped, _.mapped)

  def keyOrdering = prepared.fold(_.keyOrdering, _.keyOrdering)
  def valueOrdering = prepared.fold(_.valueOrdering: Option[Ordering[_ >: V1]], _ => None)

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]): ReduceStep[K, V1, V3] =
    IteratorMappedReduce(prepared,
      {(k: K, iter: Iterator[V1]) => fn(k, reduceFn(k, iter))},
      reducers)

  override def mapValues[V3](fn: V2 => V3): ReduceStep[K, _, V3] = {
    // don't make a closure
    val localRed = reduceFn
    IteratorMappedReduce[K,V1,V3](prepared, localRed(_, _).map(fn), reducers)
  }

  override lazy val toTypedPipe = {
    val optVSort = prepared.fold(
      {ivsr => Some(Grouped.valueSorting(ivsr.valueSort))},
      _ => None)

    val reducedPipe = groupOp { gb =>
        optVSort.map(gb.sortBy(_))
          .getOrElse(gb)
          .every(new cascading.pipe.Every(_, Grouped.valueField,
            new TypedBufferOp(reduceFn, Grouped.valueField), Fields.REPLACE))
          .reducers(reducers.getOrElse(-1))
      }
    TypedPipe.from(reducedPipe, Grouped.kvFields)(tuple2Converter[K,V2])
  }

  override def joinFunction = prepared match {
    case Left(_) =>
      sys.error("We do not support joining after value sorting. Try .toList.sorted in memory for now")
    case Right(_) =>
      // don't make a closure
      val localRed = reduceFn;
      { (kany, iter, empties) =>
        assert(empties.isEmpty, "this join function should never be called with non-empty right-most")
        localRed(kany, iter.map(_.getObject(Grouped.ValuePosition).asInstanceOf[V1]))
      }
  }
}

