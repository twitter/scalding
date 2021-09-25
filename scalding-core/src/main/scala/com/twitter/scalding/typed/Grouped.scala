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
import com.twitter.scalding.typed.functions._
import com.twitter.scalding.typed.functions.ComposedFunctions.ComposedMapGroup
import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3
import java.io.Serializable

object CoGroupable extends Serializable {
  /**
   * Return true if there is a sum occurring at the end the mapGroup transformations
   * If we know this is finally summed, we can make some different optimization choices
   *
   * If this is true, we know we have at most one value for each key
   */
  final def atMostOneValue[A, B](cg: CoGroupable[A, B]): Boolean = {
    import CoGrouped._
    cg match {
      case Pair(left, right, joinf) =>
        atMostOneValue(left) && atMostOneValue(right) && (
          joinf match {
            case Joiner.InnerJoin() => true
            case Joiner.OuterJoin() => true
            case Joiner.LeftJoin() => true
            case Joiner.RightJoin() => true
            case _ => false
          })
      case WithReducers(on, _) => atMostOneValue(on)
      case WithDescription(on, _) => atMostOneValue(on)
      case FilterKeys(on, _) => atMostOneValue(on)
      case MapGroup(on, fn) =>
        atMostOneFn(fn) || (atMostOneValue(on) && atMostInputSizeFn(fn))
      case IdentityReduce(_, _, _, _, _) => false
      case UnsortedIdentityReduce(_, _, _, _, _) => false
      case IteratorMappedReduce(_, _, fn, _, _) => atMostOneFn(fn)
    }
  }

  /**
   * Returns true if the group mapping function definitely returns 0 or 1
   * element.
   *
   * in 2.12 this can be tailrec, but the types change on recursion, so 2.11 forbids
   */
  final def atMostOneFn[A, B, C](fn: (A, Iterator[B]) => Iterator[C]): Boolean =
    fn match {
      case ComposedMapGroup(_, fn) if atMostOneFn(fn) => true
      case ComposedMapGroup(first, second) => atMostOneFn(first) && atMostInputSizeFn(second)
      case MapValueStream(SumAll(_)) => true
      case MapValueStream(ToList()) => true
      case MapValueStream(FoldIterator(_)) => true
      case MapValueStream(FoldLeftIterator(_, _)) => true
      case FoldWithKeyIterator(_) => true
      case EmptyGuard(fn) => atMostOneFn(fn)
      case _ => false
    }

  /**
   * Returns true if the group mapping function does not increase
   * the number of items in the Iterator
   */
  final def atMostInputSizeFn[A, B, C](fn: (A, Iterator[B]) => Iterator[C]): Boolean =
    fn match {
      case MapGroupMapValues(_) => true
      case MapValueStream(Drop(_)) => true
      case MapValueStream(DropWhile(_)) => true
      case MapValueStream(Take(_)) => true
      case MapValueStream(TakeWhile(_)) => true
      case FilterGroup(_) => true
      case EmptyGuard(fn) if atMostOneFn(fn) => true // since 0 always goes to 0 due to empty guard, and 1 -> 0 or 1 since atMostOne
      case EmptyGuard(fn) => atMostInputSizeFn(fn)
      case ComposedMapGroup(first, second) => atMostInputSizeFn(first) && atMostInputSizeFn(second)
      case _ => false
    }
}

/**
 * Represents something than can be CoGrouped with another CoGroupable
 */
sealed trait CoGroupable[K, +R] extends HasReducers with HasDescription with Serializable {
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
  def joinFunction: MultiJoinFunction[K, R]

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

object CoGrouped extends Serializable {
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

  def maybeCompose[A, B, C](cg: CoGrouped[A, B], rs: ReduceStep[A, B, C]): Option[CoGrouped[A, C]] = {
    val reds = com.twitter.scalding.typed.WithReducers.maybeCombine(cg.reducers, rs.reducers)

    val optCg = rs match {
      case step @ IdentityReduce(_, _, _, _, _) =>
        type Res[T] = CoGrouped[A, T]
        Some(step.evidence.subst[Res](cg))
      case step @ UnsortedIdentityReduce(_, _, _, _, _) =>
        type Res[T] = CoGrouped[A, T]
        Some(step.evidence.subst[Res](cg))
      case step @ IteratorMappedReduce(_, _, _, _, _) =>
        Some(CoGrouped.MapGroup(cg, step.reduceFn))
      case IdentityValueSortedReduce(_, _, _, _, _, _) =>
        // We can't sort after a join
        None
      case ValueSortedReduce(_, _, _, _, _, _) =>
        // We can't sort after a join
        None
    }

    optCg.map { cg1 =>
      reds match {
        case Some(r) if cg1.reducers != reds => CoGrouped.WithReducers(cg1, r)
        case _ => cg1
      }
    }
  }

  final case class Pair[K, A, B, C](
    larger: CoGroupable[K, A],
    smaller: CoGroupable[K, B],
    fn: (K, Iterator[A], Iterable[B]) => Iterator[C]) extends CoGrouped[K, C] {

    // case classes that merge more than one TypedPipe need to memoize the result or
    // it can be exponential in complexity
    override val hashCode = MurmurHash3.productHash(this)
    override def equals(that: Any) =
      that match {
        case thatRef: AnyRef if this eq thatRef => true
        case Pair(l, s, f) => (fn == f) && (l == larger) && (s == smaller)
        case _ => false
      }

    def inputs = larger.inputs ++ smaller.inputs
    def reducers = com.twitter.scalding.typed.WithReducers.maybeCombine(larger.reducers, smaller.reducers)
    def descriptions: Seq[String] = larger.descriptions ++ smaller.descriptions
    def keyOrdering = smaller.keyOrdering

    /**
     * Avoid capturing anything below as it will need to be serialized and sent to
     * all the reducers.
     */
    def joinFunction = {
      /**
       * if there is at most one value on the smaller side definitely
       * cache the result to avoid repeatedly computing it
       */
      val smallerIsAtMostOne = CoGroupable.atMostOneValue(smaller)
      if (smallerIsAtMostOne) MultiJoinFunction.PairCachedRight(larger.joinFunction, smaller.joinFunction, fn)
      else MultiJoinFunction.Pair(larger.joinFunction, smaller.joinFunction, fn)
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
    val inputs = on.inputs.map(TypedPipe.FilterKeys(_, fn))
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
    def joinFunction =
      MultiJoinFunction.MapGroup(
        on.joinFunction,
        fn)
  }
}

sealed trait CoGrouped[K, +R] extends KeyedListLike[K, R, CoGrouped]
  with CoGroupable[K, R]
  with WithReducers[CoGrouped[K, R]]
  with WithDescription[CoGrouped[K, R]]
  with Serializable {

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
    /*
     * After the join, if the key has no values, don't present it to the mapGroup
     * function. Doing so would break the invariant:
     *
     * a.join(b).toTypedPipe.group.mapGroup(fn) == a.join(b).mapGroup(fn)
     */
    CoGrouped.MapGroup(this, Grouped.addEmptyGuard(fn))

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

object HashJoinable extends Serializable {
  def toReduceStep[A, B](hj: HashJoinable[A, B]): ReduceStep[A, _, _ <: B] =
    hj match {
      case step@IdentityReduce(_, _, _, _, _) => step
      case step@UnsortedIdentityReduce(_, _, _, _, _) => step
      case step@IteratorMappedReduce(_, _, _, _, _) => step
    }

  def filterKeys[A, B](hj: HashJoinable[A, B], fn: A => Boolean): HashJoinable[A, B] =
    hj match {
      case step@IdentityReduce(_, _, _, _, _) =>
        step.copy(mapped = TypedPipe.FilterKeys(step.mapped, fn))
      case step@UnsortedIdentityReduce(_, _, _, _, _) =>
        step.copy(mapped = TypedPipe.FilterKeys(step.mapped, fn))
      case step@IteratorMappedReduce(_, _, _, _, _) =>
        step.copy(mapped = TypedPipe.FilterKeys(step.mapped, fn))
    }
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

object Grouped extends Serializable {
  def apply[K, V](pipe: TypedPipe[(K, V)])(implicit ordering: Ordering[K]): Grouped[K, V] =
    IdentityReduce[K, V, V](ordering, pipe, None, Nil, implicitly)

  def addEmptyGuard[K, V1, V2](fn: (K, Iterator[V1]) => Iterator[V2]): (K, Iterator[V1]) => Iterator[V2] =
    fn match {
      case alreadyGuarded@EmptyGuard(_) => alreadyGuarded
      case ami if CoGroupable.atMostInputSizeFn(ami) => ami // already safe
      case needGuard => EmptyGuard(needGuard)
    }
}

/**
 * All sorting methods defined here trigger Hadoop secondary sort on key + value.
 * Hadoop secondary sort is external sorting. i.e. it won't materialize all values
 * of each key in memory on the reducer.
 */
sealed trait Sortable[+T, +Sorted[+_]] {
  def withSortOrdering[U >: T](so: Ordering[U]): Sorted[U]

  def sortBy[B: Ordering](fn: (T) => B): Sorted[T] =
    withSortOrdering(Ordering.by(fn))

  // Sorts the values for each key
  def sorted[B >: T](implicit ord: Ordering[B]): Sorted[B] =
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
sealed trait ReduceStep[K, V1, V2] extends KeyedPipe[K] with HasReducers {
  /**
   * Note, this satisfies KeyedPipe.mapped: TypedPipe[(K, Any)]
   */
  def mapped: TypedPipe[(K, V1)]

  def toTypedPipe: TypedPipe[(K, V2)] = TypedPipe.ReduceStepPipe(this)
}

object ReduceStep extends Serializable {

  /**
   * assuming coherent Orderings on the A, in some cases ReduceSteps can be combined.
   * Note: we have always assumed coherant orderings in scalding with joins where
   * both sides have their own Ordering, so we argue this is not different.
   *
   * If a user has incoherant Orderings, which are already dangerous, they can
   * use .forceToDisk between reduce steps, however, a better strategy is to
   * use different key types.
   *
   * The only case where they can't is when there are two different value sorts going
   * on.
   */
  def maybeCompose[A, B, C, D](rs1: ReduceStep[A, B, C], rs2: ReduceStep[A, C, D]): Option[ReduceStep[A, B, D]] = {
    val reds = WithReducers.maybeCombine(rs1.reducers, rs2.reducers)
    val optRs = (rs1, rs2) match {
      case (step @ IdentityReduce(_, _, _, _, _), step2) =>
        type Res[T] = ReduceStep[A, T, D]
        Some(step.evidence.reverse.subst[Res](step2))
      case (step @ UnsortedIdentityReduce(_, _, _, _, _), step2) =>
        type Res[T] = ReduceStep[A, T, D]
        Some(step.evidence.reverse.subst[Res](step2))
      case (step, step2 @ IdentityReduce(_, _, _, _, _)) =>
        type Res[T] = ReduceStep[A, B, T]
        Some(step2.evidence.subst[Res](step))
      case (step, step2 @ UnsortedIdentityReduce(_, _, _, _, _)) =>
        type Res[T] = ReduceStep[A, B, T]
        Some(step2.evidence.subst[Res](step))
      case (step, step2 @ IteratorMappedReduce(_, _, _, _, _)) =>
        Some(mapGroup(step)(step2.reduceFn))
      /*
       * All the rest have either two sorts, or a sort after a reduce
       */
      case (IdentityValueSortedReduce(_, _, _, _, _, _), IdentityValueSortedReduce(_, _, _, _, _, _)) => None
      case (IdentityValueSortedReduce(_, _, _, _, _, _), ValueSortedReduce(_, _, _, _, _, _)) => None
      case (IteratorMappedReduce(_, _, _, _, _), IdentityValueSortedReduce(_, _, _, _, _, _)) => None
      case (IteratorMappedReduce(_, _, _, _, _), ValueSortedReduce(_, _, _, _, _, _)) => None
      case (ValueSortedReduce(_, _, _, _, _, _), IdentityValueSortedReduce(_, _, _, _, _, _)) => None
      case (ValueSortedReduce(_, _, _, _, _, _), ValueSortedReduce(_, _, _, _, _, _)) => None
    }

    optRs.map { composed =>
      reds.fold(composed)(withReducers(composed, _))
    }
  }

  def setInput[A, B, C](rs: ReduceStep[A, B, C], input: TypedPipe[(A, B)]): ReduceStep[A, B, C] = {
    type Res[V] = ReduceStep[A, V, C]
    type In[V] = TypedPipe[(A, V)]

    rs match {
      case step0 @ IdentityReduce(_, _, _, _, _) =>
        type IR[V] = IdentityReduce[A, V, C]
        val step = step0.evidence.subst[IR](step0)
        val revEv = step0.evidence.reverse
        val res =
          IdentityReduce[A, C, C](step.keyOrdering,
            step0.evidence.subst[In](input),
            step.reducers,
            step.descriptions,
            implicitly)
        // Put the type back to what scala expects ReduceStep[A, B, C]
        revEv.subst[Res](res)
      case step0 @ UnsortedIdentityReduce(_, _, _, _, _) =>
        type IR[V] = UnsortedIdentityReduce[A, V, C]
        val step = step0.evidence.subst[IR](step0)
        val revEv = step0.evidence.reverse
        val res =
          UnsortedIdentityReduce[A, C, C](step.keyOrdering,
            step0.evidence.subst[In](input),
            step.reducers,
            step.descriptions,
            implicitly)
        // Put the type back to what scala expects ReduceStep[A, B, C]
        revEv.subst[Res](res)
      case step0 @ IdentityValueSortedReduce(_, _, _, _, _, _) =>
        type IVSR[V] = IdentityValueSortedReduce[A, V, C]
        val step = step0.evidence.subst[IVSR](step0)
        val revEv = step0.evidence.reverse
        val res =
          IdentityValueSortedReduce[A, C, C](step.keyOrdering,
            step0.evidence.subst[In](input),
            step.valueSort,
            step.reducers,
            step.descriptions,
            implicitly)
        // Put the type back to what scala expects ReduceStep[A, B, C]
        revEv.subst[Res](res)
      case step @ ValueSortedReduce(_, _, _, _, _, _) =>
        ValueSortedReduce[A, B, C](step.keyOrdering,
          input, step.valueSort, step.reduceFn, step.reducers, step.descriptions)
      case step @ IteratorMappedReduce(_, _, _, _, _) =>
        def go(imr: IteratorMappedReduce[A, B, C]): IteratorMappedReduce[A, B, C] =
          imr.copy(mapped = input)
        go(step)
    }
  }

  def mapGroup[A, B, C, D](rs: ReduceStep[A, B, C])(fn: (A, Iterator[C]) => Iterator[D]): ReduceStep[A, B, D] =
    rs match {
      case step @ IdentityReduce(_, _, _, _, _) =>
        type Res[T] = ReduceStep[A, T, D]
        step.evidence.reverse.subst[Res](step.mapGroup(fn))
      case step @ UnsortedIdentityReduce(_, _, _, _, _) =>
        type Res[T] = ReduceStep[A, T, D]
        step.evidence.reverse.subst[Res](step.mapGroup(fn))
      case step @ IdentityValueSortedReduce(_, _, _, _, _, _) =>
        type Res[T] = ReduceStep[A, T, D]
        step.evidence.reverse.subst[Res](step.mapGroup(fn))
      case step @ ValueSortedReduce(_, _, _, _, _, _) =>
        step.mapGroup(fn)
      case step @ IteratorMappedReduce(_, _, _, _, _) =>
        step.mapGroup(fn)
    }

  def toHashJoinable[A, B, C](rs: ReduceStep[A, B, C]): Option[HashJoinable[A, C]] =
    rs match {
      case step @ IdentityReduce(_, _, _, _, _) =>
        Some(step)
      case step @ UnsortedIdentityReduce(_, _, _, _, _) =>
        Some(step)
      case step @ IteratorMappedReduce(_, _, _, _, _) =>
        Some(step)
      case step @ IdentityValueSortedReduce(_, _, _, _, _, _) =>
        None
      case step @ ValueSortedReduce(_, _, _, _, _, _) =>
        None
    }

  def withReducers[A, B, C](rs: ReduceStep[A, B, C], reds: Int): ReduceStep[A, B, C] =
    rs match {
      case step @ IdentityReduce(_, _, _, _, _) =>
        step.withReducers(reds)
      case step @ UnsortedIdentityReduce(_, _, _, _, _) =>
        step.withReducers(reds)
      case step @ IdentityValueSortedReduce(_, _, _, _, _, _) =>
        step.withReducers(reds)
      case step @ ValueSortedReduce(_, _, _, _, _, _) =>
        step.withReducers(reds)
      case step @ IteratorMappedReduce(_, _, _, _, _) =>
        step.withReducers(reds)
    }

  def withDescription[A, B, C](rs: ReduceStep[A, B, C], descr: String): ReduceStep[A, B, C] =
    rs match {
      case step @ IdentityReduce(_, _, _, _, _) =>
        step.withDescription(descr)
      case step @ UnsortedIdentityReduce(_, _, _, _, _) =>
        step.withDescription(descr)
      case step @ IdentityValueSortedReduce(_, _, _, _, _, _) =>
        step.withDescription(descr)
      case step @ ValueSortedReduce(_, _, _, _, _, _) =>
        step.withDescription(descr)
      case step @ IteratorMappedReduce(_, _, _, _, _) =>
        step.withDescription(descr)
    }
}

final case class IdentityReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int],
  override val descriptions: Seq[String],
  evidence: EqTypes[V1, V2])
  extends ReduceStep[K, V1, V2]
  with Grouped[K, V2] {

  /*
   * Because after mapValues, take, filter, we can no-longer sort,
   * we commonly convert to UnsortedIdentityReduce first, then
   * call the method there to reduce code duplication
   */
  private def toUIR = UnsortedIdentityReduce[K, V1, V2](keyOrdering, mapped, reducers, descriptions, evidence)

  private[this] def mappedV2: TypedPipe[(K, V2)] = {
    type TK[V] = TypedPipe[(K, V)]
    evidence.subst[TK](mapped)
  }

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the mappers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int) =
    toUIR.bufferedTake(n)

  override def withSortOrdering[U >: V2](so: Ordering[U]): IdentityValueSortedReduce[K, U, U] =
    IdentityValueSortedReduce[K, U, U](keyOrdering, mappedV2, so, reducers, descriptions, implicitly)

  override def withReducers(red: Int): IdentityReduce[K, V1, V2] =
    copy(reducers = Some(red))

  override def withDescription(description: String): IdentityReduce[K, V1, V2] =
    copy(descriptions = descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    toUIR.filterKeys(fn)

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    IteratorMappedReduce(keyOrdering, mappedV2, Grouped.addEmptyGuard(fn), reducers, descriptions)
  }

  // It would be nice to return IdentityReduce here, but
  // the type constraints prevent it currently
  override def mapValues[V3](fn: V2 => V3) =
    toUIR.mapValues(fn)

  // This is not correct in the type-system, but would be nice to encode
  //override def mapValues[V3](fn: V1 => V3) = IdentityReduce(keyOrdering, mapped.mapValues(fn), reducers)

  override def sum[U >: V2](implicit sg: Semigroup[U]) = {
    // there is no sort, mapValueStream or force to reducers:
    val upipe: TypedPipe[(K, U)] = mappedV2 // use covariance to set the type
    UnsortedIdentityReduce[K, U, U](keyOrdering, upipe.sumByLocalKeys, reducers, descriptions, implicitly).sumLeft
  }

  /** This is just an identity that casts the result to V2 */
  override def joinFunction = MultiJoinFunction.Casting[K, V2]
}

final case class UnsortedIdentityReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  override val reducers: Option[Int],
  override val descriptions: Seq[String],
  evidence: EqTypes[V1, V2])
  extends ReduceStep[K, V1, V2]
  with UnsortedGrouped[K, V2] {

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
      UnsortedIdentityReduce[K, V1, V2](keyOrdering, pretake, reducers, descriptions, evidence)
        .forceToReducers // jump to ValueSortedReduce
        .take(n)
    }

  override def withReducers(red: Int): UnsortedIdentityReduce[K, V1, V2] =
    copy(reducers = Some(red))

  override def withDescription(description: String): UnsortedIdentityReduce[K, V1, V2] =
    copy(descriptions = descriptions :+ description)

  override def filterKeys(fn: K => Boolean) =
    UnsortedIdentityReduce[K, V1, V2](keyOrdering, mapped.filterKeys(fn), reducers, descriptions, evidence)

  private[this] def mappedV2 = {
    type TK[V] = TypedPipe[(K, V)]
    evidence.subst[TK](mapped)
  }

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) =
    // Only pass non-Empty iterators to subsequent functions
    IteratorMappedReduce[K, V2, V3](keyOrdering, mappedV2, Grouped.addEmptyGuard(fn), reducers, descriptions)

  // It would be nice to return IdentityReduce here, but
  // the type constraints prevent it currently
  override def mapValues[V3](fn: V2 => V3) =
    UnsortedIdentityReduce[K, V3, V3](keyOrdering, mappedV2.mapValues(fn), reducers, descriptions, implicitly)

  override def sum[U >: V2](implicit sg: Semigroup[U]) = {
    // there is no sort, mapValueStream or force to reducers:
    val upipe: TypedPipe[(K, U)] = mappedV2 // use covariance to set the type
    UnsortedIdentityReduce[K, U, U](keyOrdering, upipe.sumByLocalKeys, reducers, descriptions, implicitly).sumLeft
  }

  /** This is just an identity that casts the result to V2 */
  override def joinFunction = MultiJoinFunction.Casting[K, V2]
}

final case class IdentityValueSortedReduce[K, V1, V2](
  override val keyOrdering: Ordering[K],
  override val mapped: TypedPipe[(K, V1)],
  valueSort: Ordering[V1],
  override val reducers: Option[Int],
  override val descriptions: Seq[String],
  evidence: EqTypes[V1, V2]) extends ReduceStep[K, V1, V2]
  with SortedGrouped[K, V2]
  with Reversable[IdentityValueSortedReduce[K, V1, V2]] {

  override def reverse: IdentityValueSortedReduce[K, V1, V2] =
    IdentityValueSortedReduce[K, V1, V2](keyOrdering, mapped, valueSort.reverse, reducers, descriptions, evidence)

  override def withReducers(red: Int): IdentityValueSortedReduce[K, V1, V2] =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1, V2](keyOrdering, mapped, valueSort, reducers = Some(red), descriptions, evidence)

  override def withDescription(description: String): IdentityValueSortedReduce[K, V1, V2] =
    IdentityValueSortedReduce[K, V1, V2](keyOrdering, mapped, valueSort, reducers, descriptions = descriptions :+ description, evidence)

  override def filterKeys(fn: K => Boolean) =
    // copy fails to get the types right, :/
    IdentityValueSortedReduce[K, V1, V2](keyOrdering, mapped.filterKeys(fn), valueSort, reducers, descriptions, evidence)

  override def mapGroup[V3](fn: (K, Iterator[V2]) => Iterator[V3]) = {
    // Only pass non-Empty iterators to subsequent functions
    val gfn = Grouped.addEmptyGuard(fn)
    type TK[V] = TypedPipe[(K, V)]
    ValueSortedReduce[K, V2, V3](keyOrdering, evidence.subst[TK](mapped), evidence.subst[Ordering](valueSort), gfn, reducers, descriptions)
  }

  /**
   * This does the partial heap sort followed by take in memory on the mappers
   * before sending to the reducers. This is a big help if there are relatively
   * few keys and n is relatively small.
   */
  override def bufferedTake(n: Int): SortedGrouped[K, V2] =
    if (n <= 0) {
      // This means don't take anything, which is legal, but strange
      filterKeys(Constant(false))
    } else {
      implicit val mon: PriorityQueueMonoid[V1] = new PriorityQueueMonoid[V1](n)(valueSort)
      // Do the heap-sort on the mappers:
      val pretake: TypedPipe[(K, V1)] = mapped.mapValues { v: V1 => mon.build(v) }
        .sumByLocalKeys
        .flatMap { case (k, vs) => vs.iterator.asScala.map((k, _)) }
      // Now finish on the reducers
      IdentityValueSortedReduce[K, V1, V2](keyOrdering, pretake, valueSort, reducers, descriptions, evidence)
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
  valueSort: Ordering[V1],
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
    // we don't need the empty guard here because ComposedMapGroup already does it
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
    // we don't need the empty guard here because ComposedMapGroup already does it
    val newReduce = ComposedMapGroup(reduceFn, fn)
    copy(reduceFn = newReduce)
  }

  override def joinFunction = MultiJoinFunction.MapCast(reduceFn)
}

