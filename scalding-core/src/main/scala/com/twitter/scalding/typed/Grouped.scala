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

import com.twitter.algebird.Semigroup
import com.twitter.algebird.mutable.PriorityQueueMonoid
import com.twitter.scalding.typed.functions.{ Constant, EmptyGuard }
import com.twitter.scalding.typed.functions.ComposedFunctions.ComposedMapGroup
import scala.collection.JavaConverters._

object CoGroupable {
  /*
   * This is the default empty join function needed for CoGroupable and HashJoinable
   */
  def castingJoinFunction[V]: (Any, Iterator[Any], Seq[Iterable[Any]]) => Iterator[V] =
    Joiner.CastingWideJoin[V]()
}

/**
 * Represents something than can be CoGrouped with another CoGroupable
 */
sealed trait CoGroupable[K, +R] extends HasReducers with HasDescription with java.io.Serializable {
  /**
   * This is the list of mapped pipes, just before the (reducing) joinFunction is applied
   */
  def inputs: List[TypedPipe[(K, Any)]]

  def keyOrdering: Ordering[K]

  /**
   * This function is not type-safe for others to call, but it should
   * never have an error. By construction, we never call it with incorrect
   * types.
   * It would be preferable to have stronger type safety here, but unclear
   * how to achieve, and since it is an internal function, not clear it
   * would actually help anyone for it to be type-safe
   */
  private[scalding] def joinFunction: (K, Iterator[Any], Seq[Iterable[Any]]) => Iterator[R]

  /**
   * Smaller is about average values/key not total size (that does not matter, but is
   * clearly related).
   *
   * Note that from the type signature we see that the right side is iterated (or may be)
   * over and over, but the left side is not. That means that you want the side with
   * fewer values per key on the right. If both sides are similar, no need to worry.
   * If one side is a one-to-one mapping, that should be the "smaller" side.
   */
  def cogroup[R1, R2](smaller: CoGroupable[K, R1])(fn: (K, Iterator[R], Iterable[R1]) => Iterator[R2]): CoGrouped[K, R2] =
    CoGrouped.Pair(this, smaller, fn)

  def join[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (R, W)](smaller)(Joiner.inner2)
  def leftJoin[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (R, Option[W])](smaller)(Joiner.left2)
  def rightJoin[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (Option[R], W)](smaller)(Joiner.right2)
  def outerJoin[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (Option[R], Option[W])](smaller)(Joiner.outer2)
  // TODO: implement blockJoin
}

object CoGrouped {
  // distinct by mapped, but don't reorder if the list is unique
  final def distinctBy[T, U](list: List[T])(fn: T => U): List[T] = {
    @annotation.tailrec
    def go(l: List[T], seen: Set[U] = Set[U](), acc: List[T] = Nil): List[T] = l match {
      case Nil => acc.reverse // done
      case h :: tail =>
        val uh = fn(h)
        if (seen(uh))
          go(tail, seen, acc)
        else
          go(tail, seen + uh, h :: acc)
    }
    go(list)
  }

  final case class Pair[K, A, B, C](
    larger: CoGroupable[K, A],
    smaller: CoGroupable[K, B],
    fn: (K, Iterator[A], Iterable[B]) => Iterator[C]) extends CoGrouped[K, C] {

    def inputs = larger.inputs ++ smaller.inputs
    def reducers = (larger.reducers.iterator ++ smaller.reducers.iterator).reduceOption(_ max _)
    def descriptions: Seq[String] = larger.descriptions ++ smaller.descriptions
    def keyOrdering = smaller.keyOrdering

    /**
     * Avoid capturing anything below as it will need to be serialized and sent to
     * all the reducers.
     */
    def joinFunction = {
      val leftSeqCount = larger.inputs.size - 1
      val jf = larger.joinFunction // avoid capturing `this` in the closure below
      val smallerJf = smaller.joinFunction

      { (k: K, leftMost: Iterator[Any], joins: Seq[Iterable[Any]]) =>
        val (leftSeq, rightSeq) = joins.splitAt(leftSeqCount)
        val joinedLeft = jf(k, leftMost, leftSeq)

        // Only do this once, for all calls to iterator below
        val smallerHead = rightSeq.head // linter:disable:UndesirableTypeInference
        val smallerTail = rightSeq.tail
        // TODO: it might make sense to cache this in memory as an IndexedSeq and not
        // recompute it on every value for the left if the smallerJf is non-trivial
        // we could see how long it is, and possible switch to a cached version the
        // second time through if it is small enough
        val joinedRight = new Iterable[B] {
          def iterator = smallerJf(k, smallerHead.iterator, smallerTail)
        }

        fn(k, joinedLeft, joinedRight)
      }
    }
  }

  final case class WithReducers[K, V](on: CoGrouped[K, V], reds: Int) extends CoGrouped[K, V] {
    def inputs = on.inputs
    def reducers = Some(reds)
    def keyOrdering = on.keyOrdering
    def joinFunction = on.joinFunction
    def descriptions: Seq[String] = on.descriptions
  }

  final case class WithDescription[K, V](
    on: CoGrouped[K, V],
    description: String) extends CoGrouped[K, V] {

    def inputs = on.inputs
    def reducers = on.reducers
    def keyOrdering = on.keyOrdering
    def joinFunction = on.joinFunction
    def descriptions: Seq[String] = on.descriptions :+ description
  }

  final case class FilterKeys[K, V](on: CoGrouped[K, V], fn: K => Boolean) extends CoGrouped[K, V] {
    val inputs = on.inputs.map(_.filterKeys(fn))
    def reducers = on.reducers
    def keyOrdering = on.keyOrdering
    def joinFunction = on.joinFunction
    def descriptions: Seq[String] = on.descriptions
  }

  final case class MapGroup[K, V1, V2](on: CoGrouped[K, V1], fn: (K, Iterator[V1]) => Iterator[V2]) extends CoGrouped[K, V2] {
    def inputs = on.inputs
    def reducers = on.reducers
    def descriptions: Seq[String] = on.descriptions
    def keyOrdering = on.keyOrdering
    def joinFunction = {
      val joinF = on.joinFunction // don't capture on inside the closure

      { (k: K, leftMost: Iterator[Any], joins: Seq[Iterable[Any]]) =>
        val joined = joinF(k, leftMost, joins)
        /*
         * After the join, if the key has no values, don't present it to the mapGroup
         * function. Doing so would break the invariant:
         *
         * a.join(b).toTypedPipe.group.mapGroup(fn) == a.join(b).mapGroup(fn)
         */
        Grouped.addEmptyGuard(fn)(k, joined)
      }
    }
  }
}

sealed trait CoGrouped[K, +R] extends KeyedListLike[K, R, CoGrouped]
  with CoGroupable[K, R]
  with WithReducers[CoGrouped[K, R]]
  with WithDescription[CoGrouped[K, R]]
  with java.io.Serializable {

  override def withReducers(reds: Int): CoGrouped[K, R] =
    CoGrouped.WithReducers(this, reds)

  override def withDescription(description: String): CoGrouped[K, R] =
    CoGrouped.WithDescription(this, description)

  /**
   * It seems complex to push a take up to the mappers before a general join.
   * For some cases (inner join), we could take at most n from each TypedPipe,
   * but it is not clear how to generalize that for general cogrouping functions.
   * For now, just do a normal take.
   */
  override def bufferedTake(n: Int): CoGrouped[K, R] =
    take(n)

  // Filter the keys before doing the join
  override def filterKeys(fn: K => Boolean): CoGrouped[K, R] =
    CoGrouped.FilterKeys(this, fn)

  override def mapGroup[R1](fn: (K, Iterator[R]) => Iterator[R1]): CoGrouped[K, R1] =
    CoGrouped.MapGroup(this, fn)

  override def toTypedPipe: TypedPipe[(K, R)] =
    TypedPipe.CoGroupedPipe(this)
}

/**
 * If we can HashJoin, then we can CoGroup, but not vice-versa
 * i.e., HashJoinable is a strict subset of CoGroupable (CoGrouped, for instance
 * is CoGroupable, but not HashJoinable).
 */
sealed trait HashJoinable[K, +V] extends CoGroupable[K, V] with KeyedPipe[K] {
  /** A HashJoinable has a single input into to the cogroup */
  override def inputs = List(mapped)
}
/**
 * This encodes the rules that
 * 1) sorting is only possible before doing any reduce,
 * 2) reversing is only possible after sorting.
 * 3) unsorted Groups can be CoGrouped or HashJoined
 *
 * This may appear a complex type, but it makes
 * sure that code won't compile if it breaks the rule
 */
sealed trait Grouped[K, +V]
  extends KeyedListLike[K, V, UnsortedGrouped]
  with HashJoinable[K, V]
  with Sortable[V, ({ type t[+x] = SortedGrouped[K, x] with Reversable[SortedGrouped[K, x]] })#t]
  with WithReducers[Grouped[K, V]]
  with WithDescription[Grouped[K, V]]

/**
 * After sorting, we are no longer CoGroupable, and we can only call reverse
 * in the initial SortedGrouped created from the Sortable:
 * .sortBy(_._2).reverse
 * for instance
 *
 * Once we have sorted, we cannot do a HashJoin or a CoGrouping
 */
sealed trait SortedGrouped[K, +V]
  extends KeyedListLike[K, V, SortedGrouped]
  with WithReducers[SortedGrouped[K, V]]
  with WithDescription[SortedGrouped[K, V]]

/**
 * This is the state after we have done some reducing. It is
 * not possible to sort at this phase, but it is possible to
 * do a CoGrouping or a HashJoin.
 */
sealed trait UnsortedGrouped[K, +V]
  extends KeyedListLike[K, V, UnsortedGrouped]
  with HashJoinable[K, V]
  with WithReducers[UnsortedGrouped[K, V]]
  with WithDescription[UnsortedGrouped[K, V]]

object Grouped {
  def apply[K, V](pipe: TypedPipe[(K, V)])(implicit ordering: Ordering[K]): Grouped[K, V] =
    IdentityReduce(ordering, pipe, None, Nil)

  def addEmptyGuard[K, V1, V2](fn: (K, Iterator[V1]) => Iterator[V2]): (K, Iterator[V1]) => Iterator[V2] =
    EmptyGuard(fn)
}

/**
 * All sorting methods defined here trigger Hadoop secondary sort on key + value.
 * Hadoop secondary sort is external sorting. i.e. it won't materialize all values
 * of each key in memory on the reducer.
 */
sealed trait Sortable[+T, +Sorted[+_]] {
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
sealed trait Reversable[+R] {
  def reverse: R
}

/**
 * This is a class that models the logical portion of the reduce step.
 * details like where this occurs, the number of reducers, etc... are
 * left in the Grouped class
 */
sealed trait ReduceStep[K, V1, V2] extends KeyedPipe[K] {
  /**
   * Note, this satisfies KeyedPipe.mapped: TypedPipe[(K, Any)]
   */
  def mapped: TypedPipe[(K, V1)]

  def toTypedPipe: TypedPipe[(K, V2)] = TypedPipe.ReduceStepPipe(this)
}

final case class IdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int],
  override val descriptions: Seq[String])
  extends ReduceStep[K, V1, V1]
  with Grouped[K, V1] {

  /*
   * Because after mapValues, take, filter, we can no-longer sort,
   * we commonly convert to UnsortedIdentityReduce first, then
   * call the method there to reduce code duplication
   */
  private def toUIR = UnsortedIdentityReduce(keyOrdering, mapped, reducers, descriptions)

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the mappers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int) =
    toUIR.bufferedTake(n)

  override def withSortOrdering[U >: V1](so: Ordering[U]): IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, so, reducers, descriptions)

  override def withReducers(red: Int): IdentityReduce[K, V1] =
    copy(reducers = Some(red))

  override def withDescription(description: String): IdentityReduce[K, V1] =
    copy(descriptions = descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    toUIR.filterKeys(fn)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    IteratorMappedReduce(keyOrdering, mapped, Grouped.addEmptyGuard(fn), reducers, descriptions)
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
    UnsortedIdentityReduce(keyOrdering, upipe.sumByLocalKeys, reducers, descriptions).sumLeft
  }

  /** This is just an identity that casts the result to V1 */
  override def joinFunction = CoGroupable.castingJoinFunction[V1]
}

final case class UnsortedIdentityReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int],
  override val descriptions: Seq[String])
  extends ReduceStep[K, V1, V1]
  with UnsortedGrouped[K, V1] {

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the reducers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int) =
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
      val fakeOrdering: Ordering[V1] = Ordering.by { v: V1 => v.hashCode }
      implicit val mon: PriorityQueueMonoid[V1] = new PriorityQueueMonoid[V1](n)(fakeOrdering)
      // Do the heap-sort on the mappers:
      val pretake: TypedPipe[(K, V1)] = mapped.mapValues { v: V1 => mon.build(v) }
        .sumByLocalKeys
        .flatMap { case (k, vs) => vs.iterator.asScala.map((k, _)) }
      // We have removed the priority queues, so serialization is not greater
      // Now finish on the reducers
      UnsortedIdentityReduce[K, V1](keyOrdering, pretake, reducers, descriptions)
        .forceToReducers // jump to ValueSortedReduce
        .take(n)
    }

  override def withReducers(red: Int): UnsortedIdentityReduce[K, V1] =
    copy(reducers = Some(red))

  override def withDescription(description: String): UnsortedIdentityReduce[K, V1] =
    copy(descriptions = descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    UnsortedIdentityReduce(keyOrdering, mapped.filterKeys(fn), reducers, descriptions)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    IteratorMappedReduce(keyOrdering, mapped, Grouped.addEmptyGuard(fn), reducers, descriptions)
  }

  // It would be nice to return IdentityReduce here, but
  // the type constraints prevent it currently
  override def mapValues[V2](fn: V1 => V2) =
    UnsortedIdentityReduce(keyOrdering, mapped.mapValues(fn), reducers, descriptions)

  override def sum[U >: V1](implicit sg: Semigroup[U]) = {
    // there is no sort, mapValueStream or force to reducers:
    val upipe: TypedPipe[(K, U)] = mapped // use covariance to set the type
    UnsortedIdentityReduce(keyOrdering, upipe.sumByLocalKeys, reducers, descriptions).sumLeft
  }

  /** This is just an identity that casts the result to V1 */
  override def joinFunction = CoGroupable.castingJoinFunction[V1]
}

final case class IdentityValueSortedReduce[K, V1](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[_ >: V1],
  override val reducers: Option[Int],
  override val descriptions: Seq[String]) extends ReduceStep[K, V1, V1]
  with SortedGrouped[K, V1]
  with Reversable[IdentityValueSortedReduce[K, V1]] {

  override def reverse: IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort.reverse, reducers, descriptions)

  override def withReducers(red: Int): IdentityValueSortedReduce[K, V1] =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort, reducers = Some(red), descriptions)

  override def withDescription(description: String): IdentityValueSortedReduce[K, V1] =
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped, valueSort, reducers, descriptions = descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1](keyOrdering, mapped.filterKeys(fn), valueSort, reducers, descriptions)

  override def mapGroup[V3](fn: (K, Iterator[V1]) => Iterator[V3]) =
    // Only pass non-Empty iterators to subsequent functions
    ValueSortedReduce[K, V1, V3](keyOrdering, mapped, valueSort, Grouped.addEmptyGuard(fn), reducers, descriptions)

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the reducers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int): SortedGrouped[K, V1] =
    if (n <= 0) {
      // This means don't take anything, which is legal, but strange
      filterKeys(Constant(false))
    } else {
      implicit val mon: PriorityQueueMonoid[V1] = new PriorityQueueMonoid[V1](n)(valueSort.asInstanceOf[Ordering[V1]])
      // Do the heap-sort on the mappers:
      val pretake: TypedPipe[(K, V1)] = mapped.mapValues { v: V1 => mon.build(v) }
        .sumByLocalKeys
        .flatMap { case (k, vs) => vs.iterator.asScala.map((k, _)) }
      // Now finish on the reducers
      IdentityValueSortedReduce[K, V1](keyOrdering, pretake, valueSort, reducers, descriptions)
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
}

final case class ValueSortedReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[_ >: V1],
  reduceFn: (K, Iterator[V1]) => Iterator[V2],
  override val reducers: Option[Int],
  override val descriptions: Seq[String])
  extends ReduceStep[K, V1, V2] with SortedGrouped[K, V2] {

  /**
   * After sorting, then reducing, there is no chance
   * to operate in the mappers. Just call take.
   */
  override def bufferedTake(n: Int) = take(n)

  override def withReducers(red: Int) =
    // copy infers loose types. :(
    ValueSortedReduce[K, V1, V2](
      keyOrdering, mapped, valueSort, reduceFn, Some(red), descriptions)

  override def withDescription(description: String) =
    ValueSortedReduce[K, V1, V2](
      keyOrdering, mapped, valueSort, reduceFn, reducers, descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    // copy fails to get the types right, :/
    ValueSortedReduce[K, V1, V2](keyOrdering, mapped.filterKeys(fn), valueSort, reduceFn, reducers, descriptions)

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    val newReduce = ComposedMapGroup(reduceFn, fn)
    ValueSortedReduce[K, V1, V3](
      keyOrdering, mapped, valueSort, newReduce, reducers, descriptions)
  }
}

final case class IteratorMappedReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  reduceFn: (K, Iterator[V1]) => Iterator[V2],
  override val reducers: Option[Int],
  override val descriptions: Seq[String])
  extends ReduceStep[K, V1, V2] with UnsortedGrouped[K, V2] {

  /**
   * After reducing, we are always
   * operating in memory. Just call take.
   */
  override def bufferedTake(n: Int) = take(n)

  override def withReducers(red: Int): IteratorMappedReduce[K, V1, V2] =
    copy(reducers = Some(red))

  override def withDescription(description: String): IteratorMappedReduce[K, V1, V2] =
    copy(descriptions = descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    copy(mapped = mapped.filterKeys(fn))

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    // don't make a closure
    val newReduce = ComposedMapGroup(reduceFn, fn)
    copy(reduceFn = newReduce)
  }

  override def joinFunction = {
    // don't make a closure
    val localRed = reduceFn;
    { (k, iter, empties) =>
      assert(empties.isEmpty, "this join function should never be called with non-empty right-most")
      localRed(k, iter.asInstanceOf[Iterator[V1]])
    }
  }
}

