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

import com.twitter.algebird.mutable.PriorityQueueMonoid
import com.twitter.algebird.Semigroup
import com.twitter.scalding.TupleConverter.tuple2Converter
import com.twitter.scalding.TupleSetter.tup2Setter

import com.twitter.scalding._
import com.twitter.scalding.serialization.{
  Boxed,
  BoxedOrderedSerialization,
  CascadingBinaryComparator,
  OrderedSerialization,
  WrappedSerialization
}

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.property.ConfigDef
import cascading.tuple.{ Fields, Tuple => CTuple }
import java.util.Comparator
import scala.collection.JavaConverters._
import scala.util.Try
import scala.collection.immutable.Queue

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

  def valueSorting[V](ord: Ordering[V]): Fields = Field.singleOrdered[V]("value")(ord)

  /**
   * If we are using OrderedComparable, we need to box the key
   * to prevent other serializers from handling the key
   */
  def maybeBox[K, V](ord: Ordering[K])(op: (TupleSetter[(K, V)], Fields) => Pipe): Pipe = ord match {
    case ordser: OrderedSerialization[K] =>
      val (boxfn, cls) = Boxed.next[K]
      val ts = tup2Setter[(Boxed[K], V)].contraMap { kv1: (K, V) => (boxfn(kv1._1), kv1._2) }
      val boxordSer = BoxedOrderedSerialization(boxfn, ordser)
      val keyF = new Fields("key")
      keyF.setComparator("key", new CascadingBinaryComparator(boxordSer))
      val pipe = op(ts, keyF)

      case class ToVisit[T](queue: Queue[T], inQueue: Set[T]) {
        def maybeAdd(t: T): ToVisit[T] = if (inQueue(t)) this else {
          ToVisit(queue :+ t, inQueue + t)
        }
        def next: Option[(T, ToVisit[T])] =
          if (inQueue.isEmpty) None
          else Some((queue.head, ToVisit(queue.tail, inQueue - queue.head)))
      }

      @annotation.tailrec
      def go(p: Pipe, visited: Set[Pipe], toVisit: ToVisit[Pipe]): Set[Pipe] = {
        val notSeen: Set[Pipe] = p.getPrevious.filter(i => !visited.contains(i)).toSet
        val nextVisited: Set[Pipe] = visited + p
        val nextToVisit = notSeen.foldLeft(toVisit) { case (prev, n) => prev.maybeAdd(n) }

        nextToVisit.next match {
          case Some((h, innerNextToVisit)) => go(h, nextVisited, innerNextToVisit)
          case _ => nextVisited
        }
      }

      val allPipes = go(pipe, Set[Pipe](), ToVisit[Pipe](Queue.empty, Set.empty))

      WrappedSerialization.rawSetBinary(List((cls, boxordSer)),
        {
          case (k, v) =>
            allPipes.foreach { p =>
              p.getStepConfigDef().setProperty(k + cls, v)
            }
        })
      pipe
    case _ =>
      val ts = tup2Setter[(K, V)]
      val keyF = Field.singleOrdered("key")(ord)
      op(ts, keyF)
  }

  def tuple2Conv[K, V](ord: Ordering[K]): TupleConverter[(K, V)] =
    ord match {
      case _: OrderedSerialization[_] =>
        tuple2Converter[Boxed[K], V].andThen { kv =>
          (kv._1.get, kv._2)
        }
      case _ => tuple2Converter[K, V]
    }
  def keyConverter[K](ord: Ordering[K]): TupleConverter[K] =
    ord match {
      case _: OrderedSerialization[_] =>
        TupleConverter.singleConverter[Boxed[K]].andThen(_.get)
      case _ => TupleConverter.singleConverter[K]
    }
  def keyGetter[K](ord: Ordering[K]): TupleGetter[K] =
    ord match {
      case _: OrderedSerialization[K] =>
        new TupleGetter[K] {
          def get(tup: CTuple, i: Int) = tup.getObject(i).asInstanceOf[Boxed[K]].get
        }
      case _ => TupleGetter.castingGetter
    }

  def addEmptyGuard[K, V1, V2](fn: (K, Iterator[V1]) => Iterator[V2]): (K, Iterator[V1]) => Iterator[V2] = {
    (key: K, iter: Iterator[V1]) => if (iter.nonEmpty) fn(key, iter) else Iterator.empty
  }
}

/**
 * All sorting methods defined here trigger Hadoop secondary sort on key + value.
 * Hadoop secondary sort is external sorting. i.e. it won't materialize all values
 * of each key in memory on the reducer.
 */
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
  protected def groupOp[V2](gb: GroupBuilder => GroupBuilder): TypedPipe[(K, V2)] =
    TypedPipeFactory({ (fd, mode) =>
      val pipe = Grouped.maybeBox[K, V1](keyOrdering) { (tupleSetter, fields) =>
        mapped
          .toPipe(Grouped.kvFields)(fd, mode, tupleSetter)
          .groupBy(fields)(gb)
      }
      TypedPipe.from(pipe, Grouped.kvFields)(fd, mode, Grouped.tuple2Conv[K, V2](keyOrdering))
    })
}

case class IdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int])
  extends ReduceStep[K, V1]
  with Grouped[K, V1] {

  /*
   * Because after mapValues, take, filter, we can no-longer sort,
   * we commonly convert to UnsortedIdentityReduce first, then
   * call the method there to reduce code duplication
   */
  private def toUIR = UnsortedIdentityReduce(keyOrdering, mapped, reducers)

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the mappers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int) =
    toUIR.bufferedTake(n)

  override def withSortOrdering[U >: V1](so: Ordering[U]): IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, so, reducers)

  override def withReducers(red: Int): IdentityReduce[K, V1] =
    copy(reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    toUIR.filterKeys(fn)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    IteratorMappedReduce(keyOrdering, mapped, Grouped.addEmptyGuard(fn), reducers)
  }

  // It would be nice to return IdentityReduce here, but
  // the type constraints prevent it currently
  override def mapValues[V2](fn: V1 => V2) =
    toUIR.mapValues(fn)

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
      // This is weird, but it is sometimes used to force a partition
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

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the reducers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int) =
    if (n < 1) {
      // This means don't take anything, which is legal, but strange
      filterKeys(_ => false)
    } else if (n == 1) {
      head
    } else {
      // By default, there is no ordering. This method is overridden
      // in IdentityValueSortedReduce
      // Note, this is going to bias toward low hashcode items.
      // If you care which items you take, you should sort by a random number
      // or the value itself.
      val fakeOrdering: Ordering[V1] = Ordering.by { v: V1 => v.hashCode }
      implicit val mon = new PriorityQueueMonoid[V1](n)(fakeOrdering)
      // Do the heap-sort on the mappers:
      val pretake: TypedPipe[(K, V1)] = mapped.mapValues { v: V1 => mon.build(v) }
        .sumByLocalKeys
        .flatMap { case (k, vs) => vs.iterator.asScala.map((k, _)) }
      // We have removed the priority queues, so serialization is not greater
      // Now finish on the reducers
      UnsortedIdentityReduce[K, V1](keyOrdering, pretake, reducers)
        .forceToReducers // jump to ValueSortedReduce
        .take(n)
    }

  override def withReducers(red: Int): UnsortedIdentityReduce[K, V1] =
    copy(reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    UnsortedIdentityReduce(keyOrdering, mapped.filterKeys(fn), reducers)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    IteratorMappedReduce(keyOrdering, mapped, Grouped.addEmptyGuard(fn), reducers)
  }

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
      // This is weird, but it is sometimes used to force a partition
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

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    ValueSortedReduce[K, V1, V3](keyOrdering, mapped, valueSort, Grouped.addEmptyGuard(fn), reducers)
  }

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the reducers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int): SortedGrouped[K, V1] =
    if (n <= 0) {
      // This means don't take anything, which is legal, but strange
      filterKeys(_ => false)
    } else {
      implicit val mon = new PriorityQueueMonoid[V1](n)(valueSort.asInstanceOf[Ordering[V1]])
      // Do the heap-sort on the mappers:
      val pretake: TypedPipe[(K, V1)] = mapped.mapValues { v: V1 => mon.build(v) }
        .sumByLocalKeys
        .flatMap { case (k, vs) => vs.iterator.asScala.map((k, _)) }
      // Now finish on the reducers
      IdentityValueSortedReduce[K, V1](keyOrdering, pretake, valueSort, reducers)
        .forceToReducers // jump to ValueSortedReduce
        .take(n)
    }

  /**
   * We are sorting then taking. Optimized for small take values
   * If we take <= 1, we use an in-memory-based method.
   * To force a memory-based take, use bufferedTake
   * Otherwise, we send all the values to the reducers
   */
  override def take(n: Int) =
    if (n <= 1) bufferedTake(n)
    else mapValueStream(_.take(n))

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

  /**
   * After sorting, then reducing, there is no chance
   * to operate in the mappers. Just call take.
   */
  override def bufferedTake(n: Int) = take(n)

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
    val newReduce = { (k: K, iter: Iterator[V1]) =>
      val step1 = localRed(k, iter)
      // Only pass non-Empty iterators to subsequent functions
      Grouped.addEmptyGuard(fn)(k, step1)
    }
    ValueSortedReduce[K, V1, V3](
      keyOrdering, mapped, valueSort, newReduce, reducers)
  }

  override lazy val toTypedPipe = {
    val vSort = Grouped.valueSorting(valueSort)

    groupOp {
      _.sortBy(vSort)
        .every(new cascading.pipe.Every(_, Grouped.valueField,
          new TypedBufferOp(Grouped.keyConverter(keyOrdering), reduceFn, Grouped.valueField), Fields.REPLACE))
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

  /**
   * After reducing, we are always
   * operating in memory. Just call take.
   */
  override def bufferedTake(n: Int) = take(n)

  override def withReducers(red: Int): IteratorMappedReduce[K, V1, V2] =
    copy(reducers = Some(red))

  override def filterKeys(fn: K => Boolean) =
    copy(mapped = mapped.filterKeys(fn))

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    // don't make a closure
    val localRed = reduceFn
    val newReduce = { (k: K, iter: Iterator[V1]) =>
      val step1 = localRed(k, iter)
      // Only pass non-Empty iterators to subsequent functions
      Grouped.addEmptyGuard(fn)(k, step1)
    }
    copy(reduceFn = newReduce)
  }

  override lazy val toTypedPipe =
    groupOp {
      _.every(new cascading.pipe.Every(_, Grouped.valueField,
        new TypedBufferOp(Grouped.keyConverter(keyOrdering), reduceFn, Grouped.valueField), Fields.REPLACE))
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

