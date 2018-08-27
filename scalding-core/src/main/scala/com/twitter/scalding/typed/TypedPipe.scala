/*
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

import java.io.{ OutputStream, InputStream, Serializable }

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.algebird.{ Aggregator, Batched, Monoid, Semigroup }
import com.twitter.scalding.TupleConverter.singleConverter
import com.twitter.scalding._
import com.twitter.scalding.typed.functions.{ AsLeft, AsRight, Constant, ConstantKey, DropValue1, Identity, MakeKey, GetKey, GetValue, RandomFilter, RandomNextInt, Swap, TuplizeFunction, WithConstant, PartialFunctionToFilter, SubTypes }
import com.twitter.scalding.serialization.{ OrderedSerialization, UnitOrderedSerialization }
import com.twitter.scalding.serialization.OrderedSerialization.Result
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering._
import com.stripe.dagon.{Memoize, RefPair}

import scala.util.Try
import scala.util.hashing.MurmurHash3

/**
 * factory methods for TypedPipe, which is the typed representation of distributed lists in scalding.
 * This object is here rather than in the typed package because a lot of code was written using
 * the functions in the object, which we do not see how to hide with package object tricks.
 */
object TypedPipe extends Serializable {

  /**
   * Create a TypedPipe from a cascading Pipe, some Fields and the type T
   * Avoid this if you can. Prefer from(TypedSource).
   */
  def from[T](pipe: Pipe, fields: Fields)(implicit flowDef: FlowDef, mode: Mode, conv: TupleConverter[T]): TypedPipe[T] = {

    /*
     * This could be in TypedSource, but we don't want to encourage users
     * to work directly with Pipe
     */
    case class WrappingSource[T](pipe: Pipe,
      fields: Fields,
      @transient localFlow: FlowDef, // FlowDef is not serializable. We shouldn't need to, but being paranoid
      mode: Mode,
      conv: TupleConverter[T]) extends TypedSource[T] {

      def converter[U >: T]: TupleConverter[U] =
        TupleConverter.asSuperConverter[T, U](conv)

      def read(implicit fd: FlowDef, m: Mode): Pipe = {
        // This check is not likely to fail unless someone does something really strange.
        // for historical reasons, it is not checked by the typed system
        require(m == mode,
          s"Cannot switch Mode between TypedPipe.from and toPipe calls. Pipe: $pipe, pipe mode: $m, outer mode: $mode")
        Dsl.flowDefToRichFlowDef(fd).mergeFrom(localFlow)
        pipe
      }

      override def sourceFields: Fields = fields
    }

    val localFlow = Dsl.flowDefToRichFlowDef(flowDef).onlyUpstreamFrom(pipe)
    from(WrappingSource(pipe, fields, localFlow, mode, conv))
  }

  /**
   * Create a TypedPipe from a TypedSource. This is the preferred way to make a TypedPipe
   */
  def from[T](source: TypedSource[T]): TypedPipe[T] =
    SourcePipe(source)

  /**
   * Create a TypedPipe from an Iterable in memory.
   */
  def from[T](iter: Iterable[T]): TypedPipe[T] =
    if (iter.isEmpty) empty else IterablePipe[T](iter)

  /**
   * Input must be a Pipe with exactly one Field
   * Avoid this method and prefer from(TypedSource) if possible
   */
  def fromSingleField[T](pipe: Pipe)(implicit fd: FlowDef, mode: Mode): TypedPipe[T] =
    from(pipe, new Fields(0))(fd, mode, singleConverter[T])

  /**
   * Create an empty TypedPipe. This is sometimes useful when a method must return
   * a TypedPipe, but sometimes at runtime we can check a condition and see that
   * it should be empty.
   * This is the zero of the Monoid[TypedPipe]
   */
  def empty: TypedPipe[Nothing] = EmptyTypedPipe

  /**
   * This enables pipe.hashJoin(that) or pipe.join(that) syntax
   * This is a safe enrichment because hashJoinable and CoGroupable are
   * only used in the argument position or to give cogroup, join, leftJoin, rightJoin, outerJoin
   * methods. Since those methods are unlikely to be used on TypedPipe in the future, this
   * enrichment seems safe.
   *
   * This method is the Vitaly-was-right method.
   */
  implicit def toHashJoinable[K, V](pipe: TypedPipe[(K, V)])(implicit ord: Ordering[K]): HashJoinable[K, V] =
    /*
     * Note, it would not be safe to make the return type of this Grouped[K, V] since that has some
     * different semantics than TypedPipe, however, it is not unclear when we only go to
     * HashJoinable
     */
    pipe.group

  /**
   * TypedPipe instances are monoids. They are isomorphic to multisets.
   */
  implicit def typedPipeMonoid[T]: Monoid[TypedPipe[T]] = new Monoid[TypedPipe[T]] {
    def zero = TypedPipe.empty
    def plus(left: TypedPipe[T], right: TypedPipe[T]): TypedPipe[T] =
      left ++ right
    override def sumOption(pipes: TraversableOnce[TypedPipe[T]]): Option[TypedPipe[T]] =
      if (pipes.isEmpty) None
      else {
        // we can't combine these but want to avoid a linear graph which can be slow
        // to optimize
        def combine(ps: Vector[TypedPipe[T]]): TypedPipe[T] = {
          val sz = ps.size
          if (sz == 0) TypedPipe.empty
          else if (sz == 1) ps(0)
          else {
            val left = combine(ps.take(sz/2))
            val right = combine(ps.drop(sz/2))
            left ++ right
          }
        }
        Some(combine(pipes.toVector))
      }
  }

  private val identityOrdering: OrderedSerialization[Int] = {
    val delegate = BinaryOrdering.ordSer[Int]
    new OrderedSerialization[Int] {
      override def compareBinary(a: InputStream, b: InputStream): Result = delegate.compareBinary(a, b)
      override def compare(x: Int, y: Int): Int = delegate.compare(x, y)
      override def dynamicSize(t: Int): Option[Int] = delegate.dynamicSize(t)
      override def write(out: OutputStream, t: Int): Try[Unit] = delegate.write(out, t)
      override def read(in: InputStream): Try[Int] = delegate.read(in)
      override def staticSize: Option[Int] = delegate.staticSize
      override def hash(x: Int): Int = x
    }
  }

   final case class CoGroupedPipe[K, V](@transient cogrouped: CoGrouped[K, V]) extends TypedPipe[(K, V)]
   final case class CounterPipe[A](pipe: TypedPipe[(A, Iterable[((String, String), Long)])]) extends TypedPipe[A]
   final case class CrossPipe[T, U](left: TypedPipe[T], right: TypedPipe[U]) extends TypedPipe[(T, U)] {
     def viaHashJoin: TypedPipe[(T, U)] =
       left.withKey(()).hashJoin(right.withKey(())).values
   }
   final case class CrossValue[T, U](left: TypedPipe[T], right: ValuePipe[U]) extends TypedPipe[(T, U)] {
     def viaHashJoin: TypedPipe[(T, U)] =
        right match {
          case EmptyValue =>
            EmptyTypedPipe
          case LiteralValue(v) =>
            left.map(WithConstant(v))
          case ComputedValue(pipe) =>
            CrossPipe(left, pipe)
        }
   }
   final case class DebugPipe[T](input: TypedPipe[T]) extends TypedPipe[T]
   final case class FilterKeys[K, V](input: TypedPipe[(K, V)], @transient fn: K => Boolean) extends TypedPipe[(K, V)]
   final case class Filter[T](input: TypedPipe[T], @transient fn: T => Boolean) extends TypedPipe[T]
   final case class FlatMapValues[K, V, U](input: TypedPipe[(K, V)], @transient fn: V => TraversableOnce[U]) extends TypedPipe[(K, U)]
   final case class FlatMapped[T, U](input: TypedPipe[T], @transient fn: T => TraversableOnce[U]) extends TypedPipe[U]
   final case class ForceToDisk[T](input: TypedPipe[T]) extends TypedPipe[T]
   final case class Fork[T](input: TypedPipe[T]) extends TypedPipe[T]
   final case class HashCoGroup[K, V, W, R](left: TypedPipe[(K, V)], @transient right: HashJoinable[K, W], @transient joiner: (K, V, Iterable[W]) => Iterator[R]) extends TypedPipe[(K, R)]
   final case class IterablePipe[T](iterable: Iterable[T]) extends TypedPipe[T]
   final case class MapValues[K, V, U](input: TypedPipe[(K, V)], @transient fn: V => U) extends TypedPipe[(K, U)]
   final case class Mapped[T, U](input: TypedPipe[T], @transient fn: T => U) extends TypedPipe[U]
   final case class MergedTypedPipe[T](left: TypedPipe[T], right: TypedPipe[T]) extends TypedPipe[T]
   final case class ReduceStepPipe[K, V1, V2](@transient reduce: ReduceStep[K, V1, V2]) extends TypedPipe[(K, V2)]
   final case class SourcePipe[T](@transient source: TypedSource[T]) extends TypedPipe[T]
   final case class SumByLocalKeys[K, V](input: TypedPipe[(K, V)], @transient semigroup: Semigroup[V]) extends TypedPipe[(K, V)]
   final case class TrappedPipe[T](input: TypedPipe[T], @transient sink: Source with TypedSink[T], @transient conv: TupleConverter[T]) extends TypedPipe[T]
   /**
    * descriptions carry a boolean that is true if we should deduplicate the message.
    * This is used for line numbers which are otherwise often duplicated
    */
   final case class WithDescriptionTypedPipe[T](input: TypedPipe[T], descriptions: List[(String, Boolean)]) extends TypedPipe[T]
   final case class WithOnComplete[T](input: TypedPipe[T], @transient fn: () => Unit) extends TypedPipe[T]

   case object EmptyTypedPipe extends TypedPipe[Nothing] {
     // we can't let the default TypedPipe == go here, it will stack overflow on a pattern match
     override def equals(that: Any): Boolean =
       that match {
         case e: EmptyTypedPipe.type => true
         case _ => false
       }
   }

   implicit class InvariantTypedPipe[T](val pipe: TypedPipe[T]) extends AnyVal {
     /**
      * Returns the set of distinct elements in the TypedPipe
      * This is the same as: .map((_, ())).group.sum.keys
      * If you want a distinct while joining, consider:
      * instead of:
      * {@code
      *   a.join(b.distinct.asKeys)
      * }
      * manually do the distinct:
      * {@code
      *   a.join(b.asKeys.sum)
      * }
      * The latter creates 1 map/reduce phase rather than 2
      */
     @annotation.implicitNotFound(msg = "For distinct method to work, the type in TypedPipe must have an Ordering.")
     def distinct(implicit ord: Ordering[T]): TypedPipe[T] =
       pipe.asKeys.sum.keys

     /**
       * If any errors happen below this line, but before a groupBy, write to a TypedSink
       */
      def addTrap(trapSink: Source with TypedSink[T])(implicit conv: TupleConverter[T]): TypedPipe[T] =
        TypedPipe.TrappedPipe[T](pipe, trapSink, conv).withLine
   }

   /**
    * This is where all the methods that require TypedPipe[(K, V)] live.
    *
    * previously, these were directly on TypedPipe with the use of T <:< (K, V)
    * however that complicates type inference on many functions.
    */
  implicit class Keyed[K, V](val kvpipe: TypedPipe[(K, V)]) extends AnyVal {

    /**
     * Sometimes useful for implementing custom joins with groupBy + mapValueStream when you know
     * that the value/key can fit in memory. Beware.
     */
    def eitherValues[R](that: TypedPipe[(K, R)]): TypedPipe[(K, Either[V, R])] =
      mapValues(AsLeft[V, R]()) ++ (that.mapValues(AsRight[V, R]()))

    /**
     * If T is a (K, V) for some V, then we can use this function to filter.
     * Prefer to use this if your filter only touches the key.
     *
     * This is here to match the function in KeyedListLike, where it is optimized
     */
    def filterKeys(fn: K => Boolean): TypedPipe[(K, V)] =
      TypedPipe.FilterKeys(kvpipe, fn).withLine

    /** Similar to mapValues, but allows to return a collection of outputs for each input value */
    def flatMapValues[U](f: V => TraversableOnce[U]): TypedPipe[(K, U)] =
      TypedPipe.FlatMapValues(kvpipe, f).withLine

    /**
     * flatten just the values
     * This is more useful on KeyedListLike, but added here to reduce assymmetry in the APIs
     */
    def flattenValues[U](implicit ev: V <:< TraversableOnce[U]): TypedPipe[(K, U)] = {
      val st = SubTypes.tuple2_2[K, V, TraversableOnce[U]](SubTypes.fromEv(ev))
      kvpipe.widen(st.toEv)
        .flatMapValues[U](Identity[TraversableOnce[U]]())
    }

    /**
     * This is the default means of grouping all pairs with the same key. Generally this triggers 1 Map/Reduce transition
     */
    def group(implicit ord: Ordering[K]): Grouped[K, V] =
      Grouped(kvpipe.withLine)

    /** Group using an explicit Ordering on the key. */
    def groupWith(ord: Ordering[K]): Grouped[K, V] = group(ord)

    /**
     * These operations look like joins, but they do not force any communication
     * of the current TypedPipe. They are mapping operations where this pipe is streamed
     * through one item at a time.
     *
     * WARNING These behave semantically very differently than cogroup.
     * This is because we handle (K,V) tuples on the left as we see them.
     * The iterable on the right is over all elements with a matching key K, and it may be empty
     * if there are no values for this key K.
     */
    def hashCogroup[K1 >: K, W, R](smaller: HashJoinable[K1, W])(joiner: (K1, V, Iterable[W]) => Iterator[R]): TypedPipe[(K1, R)] =
      TypedPipe.HashCoGroup(kvpipe.widen[(K1, V)], smaller, joiner).withLine

    /** Do an inner-join without shuffling this TypedPipe, but replicating argument to all tasks */
    def hashJoin[K1 >: K, W](smaller: HashJoinable[K1, W]): TypedPipe[(K1, (V, W))] =
      hashCogroup[K1, W, (V, W)](smaller)(Joiner.hashInner2)

    /** Do an leftjoin without shuffling this TypedPipe, but replicating argument to all tasks */
    def hashLeftJoin[K1 >: K, W](smaller: HashJoinable[K1, W]): TypedPipe[(K1, (V, Option[W]))] =
      hashCogroup[K1, W, (V, Option[W])](smaller)(Joiner.hashLeft2)

    /** Just keep the keys, or ._1 (if this type is a Tuple2) */
    def keys: TypedPipe[K] =
      kvpipe.map(GetKey())

    /** Transform only the values (sometimes requires giving the types due to scala type inference) */
    def mapValues[U](f: V => U): TypedPipe[(K, U)] =
      TypedPipe.MapValues(kvpipe, f).withLine

    /**
     * Enables joining when this TypedPipe has some keys with many many values and
     * but many with very few values. For instance, a graph where some nodes have
     * millions of neighbors, but most have only a few.
     *
     * We build a (count-min) sketch of each key's frequency, and we use that
     * to shard the heavy keys across many reducers.
     * This increases communication cost in order to reduce the maximum time needed
     * to complete the join.
     *
     * {@code pipe.sketch(100).join(thatPipe) }
     * will add an extra map/reduce job over a standard join to create the count-min-sketch.
     * This will generally only be beneficial if you have really heavy skew, where without
     * this you have 1 or 2 reducers taking hours longer than the rest.
     */
    def sketch(reducers: Int,
      eps: Double = 1.0E-5, //272k width = 1MB per row
      delta: Double = 0.01, //5 rows (= 5 hashes)
      seed: Int = 12345)(implicit serialization: K => Array[Byte], ordering: Ordering[K]): Sketched[K, V] =
      Sketched(kvpipe, reducers, delta, eps, seed)

    /**
     * Reasonably common shortcut for cases of associative/commutative reduction by Key
     */
    def sumByKey(implicit ord: Ordering[K], plus: Semigroup[V]): UnsortedGrouped[K, V] =
      group.sum[V]

    /**
     * This does a sum of values WITHOUT triggering a shuffle.
     * the contract is, if followed by a group.sum the result is the same
     * with or without this present, and it never increases the number of
     * items. BUT due to the cost of caching, it might not be faster if
     * there is poor key locality.
     *
     * It is only useful for expert tuning,
     * and best avoided unless you are struggling with performance problems.
     * If you are not sure you need this, you probably don't.
     *
     * The main use case is to reduce the values down before a key expansion
     * such as is often done in a data cube.
     */
    def sumByLocalKeys(implicit sg: Semigroup[V]): TypedPipe[(K, V)] =
      TypedPipe.SumByLocalKeys(kvpipe, sg).withLine

    /** swap the keys with the values */
    def swap: TypedPipe[(V, K)] =
      kvpipe.map(Swap())

    /** Just keep the values, or ._2 (if this type is a Tuple2) */
    def values: TypedPipe[V] =
      kvpipe.map(GetValue())
  }

  private case class TallyByFn[A](group: String, fn: A => String) extends Function1[A, (A, Iterable[((String, String), Long)])] {
    def apply(a: A) = (a, (((group, fn(a)), 1L)) :: Nil)
  }
  private case class TallyFn[A](group: String, counter: String) extends Function1[A, (A, Iterable[((String, String), Long)])] {
    private[this] val inc = ((group, counter), 1L) :: Nil
    def apply(a: A) = (a, inc)
  }
  private case class TallyLeft[A, B](group: String, fn: A => Either[String, B]) extends Function1[A, (List[B], Iterable[((String, String), Long)])] {
    def apply(a: A) = fn(a) match {
      case Right(b) => (b :: Nil, Nil)
      case Left(cnt) => (Nil, ((group, cnt), 1L) :: Nil)
    }
  }

  implicit class TallyEnrichment[A, B <: Iterable[((String, String), Long)]](val pipe: TypedPipe[(A, B)]) extends AnyVal {
    /**
     * Increment hadoop counters with a (group, counter) by the amount in the second
     * part of the tuple, and remove that second part
     */
    def tally: TypedPipe[A] =
      CounterPipe(pipe)
  }

  /**
   * This is a def because it allocates a new memo on each call. This is
   * important to avoid growing a memo indefinitely
   */
  private def eqFn: RefPair[TypedPipe[Any], TypedPipe[Any]] => Boolean = {

    def eqCoGroupable(left: CoGroupable[_, _], right: CoGroupable[_, _], rec: RefPair[TypedPipe[_], TypedPipe[_]] => Boolean): Boolean = {
      import CoGrouped._
      (left, right) match {
        case (Pair(la, lb, lfn), Pair(ra, rb, rfn)) =>
          (lfn == rfn) && eqCoGroupable(la, ra, rec) && eqCoGroupable(lb, rb, rec)
        case (WithReducers(left, leftRed), WithReducers(right, rightRed)) =>
          (leftRed == rightRed) && eqCoGroupable(left, right, rec)
        case (WithDescription(left, leftDesc), WithDescription(right, rightDesc)) =>
          (leftDesc == rightDesc) && eqCoGroupable(left, right, rec)
        case (CoGrouped.FilterKeys(left, lfn), CoGrouped.FilterKeys(right, rfn)) =>
          (lfn == rfn) && eqCoGroupable(left, right, rec)
        case (MapGroup(left, lfn), MapGroup(right, rfn)) =>
          (lfn == rfn) && eqCoGroupable(left, right, rec)
        case (left: ReduceStep[_, _, _], right: ReduceStep[_, _, _]) =>
          eqReduceStep(left, right, rec)
        case (_, _) => false
      }
    }

    def eqHashJoinable(left: HashJoinable[_, _], right: HashJoinable[_, _], rec: RefPair[TypedPipe[_], TypedPipe[_]] => Boolean): Boolean =
      (left, right) match {
        case (lrs: ReduceStep[_, _, _], rrs: ReduceStep[_, _, _]) =>
          eqReduceStep(lrs, rrs, rec)
      }

    def eqReduceStep(left: ReduceStep[_, _, _], right: ReduceStep[_, _, _], rec: RefPair[TypedPipe[_], TypedPipe[_]] => Boolean): Boolean = {
      val zeroLeft = ReduceStep.setInput(left, EmptyTypedPipe)
      val zeroRight = ReduceStep.setInput(right, EmptyTypedPipe)

      (zeroLeft == zeroRight) && rec(RefPair(left.mapped, right.mapped))
    }

    Memoize.function[RefPair[TypedPipe[Any], TypedPipe[Any]], Boolean] {
      case (pair, _) if pair.itemsEq => true
      case (RefPair(CoGroupedPipe(left), CoGroupedPipe(right)), rec) =>
        eqCoGroupable(left, right, rec)
      case (RefPair(CounterPipe(left), CounterPipe(right)), rec) =>
        rec(RefPair(left, right))
      case (RefPair(CrossPipe(leftA, rightA), CrossPipe(leftB, rightB)), rec) =>
        rec(RefPair(leftA, leftB)) && rec(RefPair(rightA, rightB))
      case (RefPair(CrossValue(pipeA, valueA), CrossValue(pipeB, valueB)), rec) =>
        // have to deconstruct values
        val valEq = (valueA, valueB) match {
          case (ComputedValue(pA), ComputedValue(pB)) => rec(RefPair(pA, pB))
          case (l, r) => l == r
        }
        valEq && rec(RefPair(pipeA, pipeB))
      case (RefPair(DebugPipe(left), DebugPipe(right)), rec) =>
        rec(RefPair(left, right))
      case (RefPair(FilterKeys(leftIn, leftF), FilterKeys(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(Filter(leftIn, leftF), Filter(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(FlatMapValues(leftIn, leftF), FlatMapValues(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(FlatMapped(leftIn, leftF), FlatMapped(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(ForceToDisk(left), ForceToDisk(right)), rec) =>
        rec(RefPair(left, right))
      case (RefPair(Fork(left), Fork(right)), rec) =>
        rec(RefPair(left, right))
      case (RefPair(HashCoGroup(leftA, rightA, fnA), HashCoGroup(leftB, rightB, fnB)), rec) =>
        (fnA == fnB) && rec(RefPair(leftA, leftB)) && eqHashJoinable(rightA, rightB, rec)
      case (RefPair(IterablePipe(itA), IterablePipe(itB)), _) => itA == itB
      case (RefPair(MapValues(leftIn, leftF), MapValues(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(Mapped(leftIn, leftF), Mapped(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(MergedTypedPipe(leftA, rightA), MergedTypedPipe(leftB, rightB)), rec) =>
        rec(RefPair(leftA, leftB)) && rec(RefPair(rightA, rightB))
      case (RefPair(ReduceStepPipe(left), ReduceStepPipe(right)), rec) =>
        eqReduceStep(left, right, rec)
      case (RefPair(SourcePipe(srcA), SourcePipe(srcB)), _) => srcA == srcB
      case (RefPair(SumByLocalKeys(leftIn, leftSg), SumByLocalKeys(rightIn, rightSg)), rec) =>
        (leftSg == rightSg) && rec(RefPair(leftIn, rightIn))
      case (RefPair(TrappedPipe(inA, sinkA, convA), TrappedPipe(inB, sinkB, convB)), rec) =>
        (sinkA == sinkB) && (convA == convB) && rec(RefPair(inA, inB))
      case (RefPair(WithDescriptionTypedPipe(leftIn, leftDesc), WithDescriptionTypedPipe(rightIn, rightDesc)), rec) =>
        // check the non-pipes first:
        (leftDesc == rightDesc) && rec(RefPair(leftIn, rightIn))
      case (RefPair(WithOnComplete(leftIn, leftF), WithOnComplete(rightIn, rightF)), rec) =>
        // check the non-pipes first:
        (leftF == rightF) && rec(RefPair(leftIn, rightIn))
      case (RefPair(EmptyTypedPipe, EmptyTypedPipe), _) => true
      case _ => false // we don't match on which subtype we are
    }
  }
}

/**
 * Think of a TypedPipe as a distributed unordered list that may or may not yet
 * have been materialized in memory or disk.
 *
 * Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
sealed abstract class TypedPipe[+T] extends Serializable with Product {

  override val hashCode: Int = MurmurHash3.productHash(this)
  override def equals(that: Any): Boolean = that match {
    case thatTP: TypedPipe[_] =>
      if (thatTP eq this) true
      else if (thatTP.hashCode != hashCode) false // since we have a cached hashCode, use it
      else {
        // we only check this in the case of true equality without reference
        // equality or rarely due to hash collisions. So we can expect to
        // walk the entire graph in most cases where we get here.
        // Without the memoization below, that graph walking can
        // be exponentially slow. With the memoization, it becomes O(N)
        // where N is the size of the reachable graph distinct by reference
        // equality
        val fn = TypedPipe.eqFn
        fn(RefPair(this, thatTP))
      }
    case _ => false
  }

  protected def withLine: TypedPipe[T] =
    LineNumber.tryNonScaldingCaller.map(_.toString) match {
      case None =>
        this
      case Some(desc) =>
        TypedPipe.WithDescriptionTypedPipe(this, (desc, true) :: Nil) // deduplicate line numbers
    }

  /**
   * Increment diagnostic counters by 1 for each item in the pipe.
   * The counter group will be the same for each item, the counter name
   * is determined by the result of the `fn` passed in.
   */
  def tallyBy(group: String)(fn: T => String): TypedPipe[T] =
    map(TypedPipe.TallyByFn(group, fn)).tally

  /**
   * Increment a specific diagnostic counter by 1 for each item in the pipe.
   *
   * this is the same as tallyBy(group)(_ => counter)
   */
  def tallyAll(group: String, counter: String): TypedPipe[T] =
    map(TypedPipe.TallyFn(group, counter)).tally

  /**
   * Increment a diagnostic counter for each failure. This is like map,
   * where the `fn` should return a `Right[U]` for each successful transformation
   * and a `Left[String]` for each failure, with the String describing the failure.
   * Each failure will be counted, and the result is just the successes.
   */
  def tallyLeft[B](group: String)(fn: T => Either[String, B]): TypedPipe[B] =
    map(TypedPipe.TallyLeft(group, fn)).tally.flatten

  /**
   * Implements a cross product.  The right side should be tiny
   * This gives the same results as
   * {code for { l <- list1; l2 <- list2 } yield (l, l2) }
   */
  def cross[U](tiny: TypedPipe[U]): TypedPipe[(T, U)] =
    TypedPipe.CrossPipe(this, tiny).withLine

  /**
   * This is the fundamental mapper operation.
   * It behaves in a way similar to List.flatMap, which means that each
   * item is fed to the input function, which can return 0, 1, or many outputs
   * (as a TraversableOnce) per input. The returned results will be iterated through once
   * and then flattened into a single TypedPipe which is passed to the next step in the
   * pipeline.
   *
   * This behavior makes it a powerful operator -- it can be used to filter records
   * (by returning 0 items for a given input), it can be used the way map is used
   * (by returning 1 item per input), it can be used to explode 1 input into many outputs,
   * or even a combination of all of the above at once.
   */
  def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    TypedPipe.FlatMapped(this, f).withLine

  /**
   * Export back to a raw cascading Pipe. useful for interop with the scalding
   * Fields API or with Cascading code.
   * Avoid this if possible. Prefer to write to TypedSink.
   */
  final def toPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe =
    // we have to be cafeful to pass the setter we want since a low priority implicit can always be
    // found :(
    cascading_backend.CascadingBackend.toPipe[U](withLine, fieldNames)(flowDef, mode, setter)

  /**
   * Merge two TypedPipes (no order is guaranteed)
   * This is only realized when a group (or join) is
   * performed.
   */
  def ++[U >: T](other: TypedPipe[U]): TypedPipe[U] =
    TypedPipe.MergedTypedPipe(this, other).withLine

  /**
   * Aggregate all items in this pipe into a single ValuePipe
   *
   * Aggregators are composable reductions that allow you to glue together
   * several reductions and process them in one pass.
   *
   * Same as groupAll.aggregate.values
   */
  def aggregate[B, C](agg: Aggregator[T, B, C]): ValuePipe[C] =
    ComputedValue(groupAll.aggregate(agg).values)

  /**
   * Put the items in this into the keys, and unit as the value in a Group
   * in some sense, this is the dual of groupAll
   */
  @annotation.implicitNotFound(msg = "For asKeys method to work, the type in TypedPipe must have an Ordering.")
  def asKeys[U >: T](implicit ord: Ordering[U]): Grouped[U, Unit] =
    widen[U]
      .withValue(())
      .group

  /**
   * Set a key to to the given value.
   */
  def withKey[K](key: K): TypedPipe[(K, T)] =
    map(ConstantKey(key))

  /**
   * Set a key to to the given value.
   */
  def withValue[V](value: V): TypedPipe[(T, V)] =
    map(WithConstant(value))

  /**
   * If T <:< U, then this is safe to treat as TypedPipe[U] due to covariance
   */
  def widen[U](implicit ev: T <:< U): TypedPipe[U] =
    SubTypes.fromEv(ev).liftCo[TypedPipe](this)

  /**
   * Filter and map. See scala.collection.List.collect.
   * {@code
   *   collect { case Some(x) => fn(x) }
   * }
   */
  def collect[U](fn: PartialFunction[T, U]): TypedPipe[U] =
    filter(PartialFunctionToFilter(fn)).map(fn)

  /**
   * Attach a ValuePipe to each element this TypedPipe
   */
  def cross[V](p: ValuePipe[V]): TypedPipe[(T, V)] =
    TypedPipe.CrossValue(this, p).withLine

  /** prints the current pipe to stdout */
  def debug: TypedPipe[T] =
    TypedPipe.DebugPipe(this).withLine

  /** adds a description to the pipe */
  def withDescription(description: String): TypedPipe[T] =
    TypedPipe.WithDescriptionTypedPipe[T](this, (description, false) :: Nil)

  /**
   * Returns the set of distinct elements identified by a given lambda extractor in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinctBy method to work, the type to distinct on in the TypedPipe must have an Ordering.")
  def distinctBy[U](fn: T => U, numReducers: Option[Int] = None)(implicit ord: Ordering[U]): TypedPipe[T] = {
    val op = groupBy(fn).head
    val reduced = numReducers match {
      case Some(red) => op.withReducers(red)
      case None => op
    }
    reduced.map(GetValue())
  }

  /** Merge two TypedPipes of different types by using Either */
  def either[R](that: TypedPipe[R]): TypedPipe[Either[T, R]] =
    map(AsLeft()) ++ (that.map(AsRight()))

  /**
   * If you are going to create two branches or forks,
   * it may be more efficient to call this method first
   * which will create a node in the cascading graph.
   * Without this, both full branches of the fork will be
   * put into separate cascading pipes, which can, in some cases,
   * be slower.
   *
   * Ideally the planner would see this
   */
  def fork: TypedPipe[T] = TypedPipe.Fork(this).withLine

  /**
   * limit the output to at most count items, if at least count items exist.
   */
  def limit(count: Int): TypedPipe[T] =
    groupAll.bufferedTake(count).values

  /** Transform each element via the function f */
  def map[U](f: T => U): TypedPipe[U] =
    TypedPipe.Mapped(this, f).withLine

  /**
   * Keep only items that satisfy this predicate
   */
  def filter(f: T => Boolean): TypedPipe[T] =
    TypedPipe.Filter(this, f).withLine

  // This is just to appease for comprehension
  def withFilter(f: T => Boolean): TypedPipe[T] = filter(f)

  /**
   * Keep only items that don't satisfy the predicate.
   * `filterNot` is the same as `filter` with a negated predicate.
   */
  def filterNot(f: T => Boolean): TypedPipe[T] =
    filter(!f(_))

  /** flatten an Iterable */
  def flatten[U](implicit ev: T <:< TraversableOnce[U]): TypedPipe[U] =
    widen[TraversableOnce[U]].flatMap(Identity[TraversableOnce[U]]())

  /**
   * Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   * This is useful for experts who see some heuristic of the planner causing
   * slower performance.
   */
  def forceToDisk: TypedPipe[T] =
    TypedPipe.ForceToDisk(this).withLine


  /** Send all items to a single reducer */
  def groupAll: Grouped[Unit, T] =
    groupBy(Constant(()))(UnitOrderedSerialization).withReducers(1)

  /** Given a key function, add the key, then call .group */
  def groupBy[K](g: T => K)(implicit ord: Ordering[K]): Grouped[K, T] =
    map(MakeKey(g)).group

  /**
   * Forces a shuffle by randomly assigning each item into one
   * of the partitions.
   *
   * This is for the case where you mappers take a long time, and
   * it is faster to shuffle them to more reducers and then operate.
   *
   * You probably want shard if you are just forcing a shuffle.
   */
  def groupRandomly(partitions: Int): Grouped[Int, T] =
    groupBy(RandomNextInt(123, partitions))(TypedPipe.identityOrdering)
      .withReducers(partitions)

  /**
   * Partitions this into two pipes according to a predicate.
   *
   * Sometimes what you really want is a groupBy in these cases.
   */
  def partition(p: T => Boolean): (TypedPipe[T], TypedPipe[T]) = {
    val forked = fork
    (forked.filter(p), forked.filterNot(p))
  }

  private[this] def defaultSeed: Long = System.identityHashCode(this) * 2654435761L ^ System.currentTimeMillis
  /**
   * Sample a fraction (between 0 and 1) uniformly independently at random each element of the pipe
   * does not require a reduce step.
   * This method makes sure to fix the seed, otherwise restarts cause subtle errors.
   */
  def sample(fraction: Double): TypedPipe[T] = sample(fraction, defaultSeed)

  /**
   * Sample a fraction (between 0 and 1) uniformly independently at random each element of the pipe with
   * a given seed.
   * Does not require a reduce step.
   */
  def sample(fraction: Double, seed: Long): TypedPipe[T] = {
    require(0.0 <= fraction && fraction <= 1.0, s"got $fraction which is an invalid fraction")
    filter(RandomFilter(seed, fraction))
  }

  /**
   * Used to force a shuffle into a given size of nodes.
   * Only use this if your mappers are taking far longer than
   * the time to shuffle.
   */
  def shard(partitions: Int): TypedPipe[T] = groupRandomly(partitions).forceToReducers.values

  /**
   * Reasonably common shortcut for cases of total associative/commutative reduction
   * returns a ValuePipe with only one element if there is any input, otherwise EmptyValue.
   */
  def sum[U >: T](implicit plus: Semigroup[U]): ValuePipe[U] = {
    // every 1000 items, compact.
    lazy implicit val batchedSG: Semigroup[Batched[U]] = Batched.compactingSemigroup[U](1000)
    // TODO: literals like this defeat caching in the planner
    ComputedValue(map { t => ((), Batched[U](t)) }
      .sumByLocalKeys
      // remove the Batched before going to the reducers
      // TODO: literals like this defeat caching in the planner
      .map { case (_, batched) => batched.sum }
      .groupAll
      .forceToReducers
      .sum
      .values)
  }

  /**
   * This is used when you are working with Execution[T] to create loops.
   * You might do this to checkpoint and then flatMap Execution to continue
   * from there. Probably only useful if you need to flatMap it twice to fan
   * out the data into two children jobs.
   *
   * This writes the current TypedPipe into a temporary file
   * and then opens it after complete so that you can continue from that point
   */
  def forceToDiskExecution: Execution[TypedPipe[T]] =
    Execution.forceToDisk(this)

  /**
   * This gives an Execution that when run evaluates the TypedPipe,
   * writes it to disk, and then gives you an Iterable that reads from
   * disk on the submit node each time .iterator is called.
   * Because of how scala Iterables work, mapping/flatMapping/filtering
   * the Iterable forces a read of the entire thing. If you need it to
   * be lazy, call .iterator and use the Iterator inside instead.
   */
  def toIterableExecution: Execution[Iterable[T]] =
    Execution.toIterable(this)

  /** use a TupleUnpacker to flatten U out into a cascading Tuple */
  def unpackToPipe[U >: T](fieldNames: Fields)(implicit fd: FlowDef, mode: Mode, up: TupleUnpacker[U]): Pipe = {
    val setter = up.newSetter(fieldNames)
    toPipe[U](fieldNames)(fd, mode, setter)
  }

  /**
   * This attaches a function that is called at the end of the map phase on
   * EACH of the tasks that are executing.
   * This is for expert use only. You probably won't ever need it. Try hard
   * to avoid it. Execution also has onComplete that can run when an Execution
   * has completed.
   */
  def onComplete(fn: () => Unit): TypedPipe[T] =
    TypedPipe.WithOnComplete[T](this, fn).withLine

  /**
   * Safely write to a TypedSink[T]. If you want to write to a Source (not a Sink)
   * you need to do something like: toPipe(fieldNames).write(dest)
   * @return a pipe equivalent to the current pipe.
   */
  def write(dest: TypedSink[T])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] = {
    // We do want to record the line number that this occurred at
    val next = withLine
    FlowStateMap.merge(flowDef, FlowState.withTypedWrite(next, dest, mode))
    next
  }

  /**
   * This is the functionally pure approach to building jobs. Note,
   * that you have to call run on the result or flatMap/zip it
   * into an Execution that is run for anything to happen here.
   */
  def writeExecution(dest: TypedSink[T]): Execution[Unit] =
    Execution.write(this, dest)

  /**
   * If you want to write to a specific location, and then read from
   * that location going forward, use this.
   */
  def writeThrough[U >: T](dest: TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
    Execution.write(this, dest, TypedPipe.from(dest))

  /**
   * If you want to writeThrough to a specific file if it doesn't already exist,
   * and otherwise just read from it going forward, use this.
   */
  def make[U >: T](dest: Source with TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
    Execution.getMode.flatMap { mode =>
      try {
        dest.validateTaps(mode)
        Execution.from(TypedPipe.from(dest))
      } catch {
        case ivs: InvalidSourceException => writeThrough(dest)
      }
    }


  /**
   * ValuePipe may be empty, so, this attaches it as an Option
   * cross is the same as leftCross(p).collect { case (t, Some(v)) => (t, v) }
   */
  def leftCross[V](p: ValuePipe[V]): TypedPipe[(T, Option[V])] =
    p match {
      case EmptyValue => map(WithConstant(None))
      case LiteralValue(v) => map(WithConstant(Some(v)))
      case ComputedValue(pipe) => leftCross(pipe)
    }

  /** uses hashJoin but attaches None if thatPipe is empty */
  def leftCross[V](thatPipe: TypedPipe[V]): TypedPipe[(T, Option[V])] =
    withKey(()).hashLeftJoin(thatPipe.withKey(())).values

  /**
   * common pattern of attaching a value and then map
   * recommended style:
   * {@code
   *  mapWithValue(vpu) {
   *    case (t, Some(u)) => op(t, u)
   *    case (t, None) => // if you never expect this:
   *      sys.error("unexpected empty value pipe")
   *  }
   * }
   */
  def mapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => V): TypedPipe[V] =
    leftCross(value).map(TuplizeFunction(f))

  /**
   * common pattern of attaching a value and then flatMap
   * recommended style:
   * {@code
   *  flatMapWithValue(vpu) {
   *    case (t, Some(u)) => op(t, u)
   *    case (t, None) => // if you never expect this:
   *      sys.error("unexpected empty value pipe")
   *  }
   * }
   */
  def flatMapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => TraversableOnce[V]): TypedPipe[V] =
    leftCross(value).flatMap(TuplizeFunction(f))

  /**
   * common pattern of attaching a value and then filter
   * recommended style:
   * {@code
   *  filterWithValue(vpu) {
   *    case (t, Some(u)) => op(t, u)
   *    case (t, None) => // if you never expect this:
   *      sys.error("unexpected empty value pipe")
   *  }
   * }
   */
  def filterWithValue[U](value: ValuePipe[U])(f: (T, Option[U]) => Boolean): TypedPipe[T] =
    leftCross(value).filter(TuplizeFunction(f)).map(GetKey())


  /**
   * For each element, do a map-side (hash) left join to look up a value
   */
  def hashLookup[K >: T, V](grouped: HashJoinable[K, V]): TypedPipe[(K, Option[V])] =
    map(WithConstant(()))
      .widen[(K, Unit)]
      .hashLeftJoin(grouped)
      .map(DropValue1())

}

/**
 * This class is for the syntax enrichment enabling
 * .joinBy on TypedPipes. To access this, do
 * import Syntax.joinOnMappablePipe
 */
class MappablePipeJoinEnrichment[T](pipe: TypedPipe[T]) {
  def joinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (T, U)] = pipe.groupBy(g).withReducers(reducers).join(smaller.groupBy(h))
  def leftJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (T, Option[U])] = pipe.groupBy(g).withReducers(reducers).leftJoin(smaller.groupBy(h))
  def rightJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (Option[T], U)] = pipe.groupBy(g).withReducers(reducers).rightJoin(smaller.groupBy(h))
  def outerJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (Option[T], Option[U])] = pipe.groupBy(g).withReducers(reducers).outerJoin(smaller.groupBy(h))
}

/**
 * These are named syntax extensions that users can optionally import.
 * Avoid import Syntax._
 */
object Syntax {
  implicit def joinOnMappablePipe[T](p: TypedPipe[T]): MappablePipeJoinEnrichment[T] = new MappablePipeJoinEnrichment(p)
}
