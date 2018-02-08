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
import java.util.UUID

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.algebird.{ Aggregator, Batched, Monoid, Semigroup }
import com.twitter.scalding.TupleConverter.singleConverter
import com.twitter.scalding._
import com.twitter.scalding.typed.functions.{ AsLeft, AsRight, Constant, DropValue1, EqTypes, Identity, MakeKey, GetKey, GetValue, RandomFilter, RandomNextInt, Swap, TuplizeFunction, WithConstant, PartialFunctionToFilter, SubTypes }
import com.twitter.scalding.serialization.{ OrderedSerialization, UnitOrderedSerialization }
import com.twitter.scalding.serialization.OrderedSerialization.Result
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering._

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

   final case class CoGroupedPipe[K, V](cogrouped: CoGrouped[K, V]) extends TypedPipe[(K, V)] {
     //
     // Very long chains of maps are uncommon. There are generally
     // periodic reduces or joins. we cache hashCode and optimize equality
     // here to improve using TypedPipe in hash tables
     //
     override val hashCode = MurmurHash3.productHash(this)
     override def equals(that: Any) =
       that match {
         case thatRef: AnyRef if this eq thatRef => true
         case CoGroupedPipe(cg) => cg == cogrouped
         case _ => false
       }
   }
   final case class CounterPipe[A](pipe: TypedPipe[(A, Iterable[((String, String), Long)])]) extends TypedPipe[A]
   final case class CrossPipe[T, U](left: TypedPipe[T], right: TypedPipe[U]) extends TypedPipe[(T, U)] {
     def viaHashJoin: TypedPipe[(T, U)] =
       left.groupAll.hashJoin(right.groupAll).values

     // case classes that merge more than one TypedPipe need to memoize the result or
     // it can be exponential in complexity
     override val hashCode = MurmurHash3.productHash(this)
     override def equals(that: Any) =
       that match {
         case thatRef: AnyRef if this eq thatRef => true
         case CrossPipe(l, r) => (l == left) && (r == right)
         case _ => false
       }
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
     // case classes that merge more than one TypedPipe need to memoize the result or
     // it can be exponential in complexity
     override val hashCode = MurmurHash3.productHash(this)
     override def equals(that: Any) =
       that match {
         case thatRef: AnyRef if this eq thatRef => true
         case CrossValue(l, r) => (l == left) && (r == right)
         case _ => false
       }
   }
   final case class DebugPipe[T](input: TypedPipe[T]) extends TypedPipe[T]
   final case class FilterKeys[K, V](input: TypedPipe[(K, V)], fn: K => Boolean) extends TypedPipe[(K, V)]
   final case class Filter[T](input: TypedPipe[T], fn: T => Boolean) extends TypedPipe[T]
   final case class FlatMapValues[K, V, U](input: TypedPipe[(K, V)], fn: V => TraversableOnce[U]) extends TypedPipe[(K, U)]
   final case class FlatMapped[T, U](input: TypedPipe[T], fn: T => TraversableOnce[U]) extends TypedPipe[U]
   final case class ForceToDisk[T](input: TypedPipe[T]) extends TypedPipe[T]
   final case class Fork[T](input: TypedPipe[T]) extends TypedPipe[T]
   final case class HashCoGroup[K, V, W, R](left: TypedPipe[(K, V)], right: HashJoinable[K, W], joiner: (K, V, Iterable[W]) => Iterator[R]) extends TypedPipe[(K, R)] {
     // case classes that merge more than one TypedPipe need to memoize the result or
     // it can be exponential in complexity
     override val hashCode = MurmurHash3.productHash(this)
     override def equals(that: Any) =
       that match {
         case thatRef: AnyRef if this eq thatRef => true
         case HashCoGroup(l, r, j) => (j == joiner) && (l == left) && (r == right)
         case _ => false
       }
   }
   final case class IterablePipe[T](iterable: Iterable[T]) extends TypedPipe[T]
   final case class MapValues[K, V, U](input: TypedPipe[(K, V)], fn: V => U) extends TypedPipe[(K, U)]
   final case class Mapped[T, U](input: TypedPipe[T], fn: T => U) extends TypedPipe[U]
   final case class MergedTypedPipe[T](left: TypedPipe[T], right: TypedPipe[T]) extends TypedPipe[T] {
     // case classes that merge more than one TypedPipe need to memoize the result or
     // it can be exponential in complexity
     override val hashCode = MurmurHash3.productHash(this)
     override def equals(that: Any) =
       that match {
         case thatRef: AnyRef if this eq thatRef => true
         case MergedTypedPipe(l, r) => (l == left) && (r == right)
         case _ => false
       }
   }
   final case class ReduceStepPipe[K, V1, V2](reduce: ReduceStep[K, V1, V2]) extends TypedPipe[(K, V2)]{
     //
     // Very long chains of maps are uncommon. There are generally
     // periodic reduces or joins. we cache hashCode and optimize equality
     // here to improve using TypedPipe in hash tables
     //
     override val hashCode = MurmurHash3.productHash(this)
     override def equals(that: Any) =
       that match {
         case thatRef: AnyRef if this eq thatRef => true
         case ReduceStepPipe(r) => r == reduce
         case _ => false
       }
   }
   final case class SourcePipe[T](source: TypedSource[T]) extends TypedPipe[T]
   final case class SumByLocalKeys[K, V](input: TypedPipe[(K, V)], semigroup: Semigroup[V]) extends TypedPipe[(K, V)]
   final case class TrappedPipe[T](input: TypedPipe[T], sink: Source with TypedSink[T], conv: TupleConverter[T]) extends TypedPipe[T]
   final case class WithDescriptionTypedPipe[T](input: TypedPipe[T], description: String, deduplicate: Boolean) extends TypedPipe[T]
   final case class WithOnComplete[T](input: TypedPipe[T], fn: () => Unit) extends TypedPipe[T]

   case object EmptyTypedPipe extends TypedPipe[Nothing]

   implicit class InvariantTypedPipe[T](val pipe: TypedPipe[T]) extends AnyVal {
      /**
       * If any errors happen below this line, but before a groupBy, write to a TypedSink
       */
      def addTrap(trapSink: Source with TypedSink[T])(implicit conv: TupleConverter[T]): TypedPipe[T] =
        TypedPipe.TrappedPipe[T](pipe, trapSink, conv).withLine
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
   * This is an unsafe function in general, but safe as we use it here.
   * Ordering is invariant because it has functions like max and min which
   * accept and return items of the same type. If we had a proof that such
   * functions had the law: max(a, b) eq a || max(a, b) eq b, which is true
   * in almost any implementation, then this method is safe to cast the Ordering.
   *
   * since we don't actually use max/min directly on Ordering, but only
   * use the contravariant methods, this is safe as a cast.
   */
  private[scalding] def narrowOrdering[A, B <: A](ordA: Ordering[A]): Ordering[B] =
    // this compiles, but is potentially slower
    // ordA.on { a: B => (a: A) }
    // cast because Ordering is not contravariant, but should be (and this cast is safe)
    ordA.asInstanceOf[Ordering[B]]
}

/**
 * Think of a TypedPipe as a distributed unordered list that may or may not yet
 * have been materialized in memory or disk.
 *
 * Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
sealed abstract class TypedPipe[+T] extends Serializable {

  protected def withLine: TypedPipe[T] =
    LineNumber.tryNonScaldingCaller.map(_.toString) match {
      case None =>
        this
      case Some(desc) =>
        TypedPipe.WithDescriptionTypedPipe(this, desc, true) // deduplicate line numbers
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
    map(WithConstant(())).group

  /**
   * If T <:< U, then this is safe to treat as TypedPipe[U] due to covariance
   */
  protected def raiseTo[U](implicit ev: T <:< U): TypedPipe[U] =
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
    TypedPipe.WithDescriptionTypedPipe[T](this, description, false)

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
  def distinct(implicit ord: Ordering[_ >: T]): TypedPipe[T] =
    asKeys[T](TypedPipe.narrowOrdering(ord)).sum.keys

  /**
   * Returns the set of distinct elements identified by a given lambda extractor in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinctBy method to work, the type to distinct on in the TypedPipe must have an Ordering.")
  def distinctBy[U](fn: T => U, numReducers: Option[Int] = None)(implicit ord: Ordering[_ >: U]): TypedPipe[T] = {
    implicit val ordT: Ordering[U] = TypedPipe.narrowOrdering(ord)

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
   * Sometimes useful for implementing custom joins with groupBy + mapValueStream when you know
   * that the value/key can fit in memory. Beware.
   */
  def eitherValues[K, V, R](that: TypedPipe[(K, R)])(implicit ev: T <:< (K, V)): TypedPipe[(K, Either[V, R])] =
    mapValues(AsLeft[V, R]()) ++ (that.mapValues(AsRight[V, R]()))

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

  /** Transform only the values (sometimes requires giving the types due to scala type inference) */
  def mapValues[K, V, U](f: V => U)(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    TypedPipe.MapValues(raiseTo[(K, V)], f).withLine

  /** Similar to mapValues, but allows to return a collection of outputs for each input value */
  def flatMapValues[K, V, U](f: V => TraversableOnce[U])(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    TypedPipe.FlatMapValues(raiseTo[(K, V)], f).withLine

  /**
   * Keep only items that satisfy this predicate
   */
  def filter(f: T => Boolean): TypedPipe[T] =
    TypedPipe.Filter(this, f).withLine

  // This is just to appease for comprehension
  def withFilter(f: T => Boolean): TypedPipe[T] = filter(f)

  /**
   * If T is a (K, V) for some V, then we can use this function to filter.
   * Prefer to use this if your filter only touches the key.
   *
   * This is here to match the function in KeyedListLike, where it is optimized
   */
  def filterKeys[K, V](fn: K => Boolean)(implicit ev: T <:< (K, V)): TypedPipe[(K, V)] =
    TypedPipe.FilterKeys(raiseTo[(K, V)], fn).withLine

  /**
   * Keep only items that don't satisfy the predicate.
   * `filterNot` is the same as `filter` with a negated predicate.
   */
  def filterNot(f: T => Boolean): TypedPipe[T] =
    filter(!f(_))

  /** flatten an Iterable */
  def flatten[U](implicit ev: T <:< TraversableOnce[U]): TypedPipe[U] =
    raiseTo[TraversableOnce[U]].flatMap(Identity[TraversableOnce[U]]())

  /**
   * flatten just the values
   * This is more useful on KeyedListLike, but added here to reduce assymmetry in the APIs
   */
  def flattenValues[K, U](implicit ev: T <:< (K, TraversableOnce[U])): TypedPipe[(K, U)] =
    flatMapValues[K, TraversableOnce[U], U](Identity[TraversableOnce[U]]())

  /**
   * Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   * This is useful for experts who see some heuristic of the planner causing
   * slower performance.
   */
  def forceToDisk: TypedPipe[T] =
    TypedPipe.ForceToDisk(this).withLine

  /**
   * This is the default means of grouping all pairs with the same key. Generally this triggers 1 Map/Reduce transition
   */
  def group[K, V](implicit ev: <:<[T, (K, V)], ord: Ordering[K]): Grouped[K, V] =
    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.
    Grouped(raiseTo[(K, V)].withLine)

  /** Send all items to a single reducer */
  def groupAll: Grouped[Unit, T] =
    groupBy(Constant(()))(UnitOrderedSerialization).withReducers(1)

  /** Given a key function, add the key, then call .group */
  def groupBy[K](g: T => K)(implicit ord: Ordering[K]): Grouped[K, T] =
  map(MakeKey(g)).group

  /** Group using an explicit Ordering on the key. */
  def groupWith[K, V](ord: Ordering[K])(implicit ev: <:<[T, (K, V)]): Grouped[K, V] = group(ev, ord)

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
  def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]): TypedPipe[(K, V)] =
    TypedPipe.SumByLocalKeys(raiseTo[(K, V)], sg).withLine

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
   * Reasonably common shortcut for cases of associative/commutative reduction by Key
   */
  def sumByKey[K, V](implicit ev: T <:< (K, V), ord: Ordering[K], plus: Semigroup[V]): UnsortedGrouped[K, V] =
    group[K, V].sum[V]

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
    // We do want to record the line number that this occured at
    val next = withLine
    FlowStateMap.mutate(flowDef) { fs =>
      (fs.addTypedWrite(next, dest, mode), ())
    }
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

  /** Just keep the keys, or ._1 (if this type is a Tuple2) */
  def keys[K](implicit ev: <:<[T, (K, Any)]): TypedPipe[K] =
    raiseTo[(K, Any)].map(GetKey())

  /** swap the keys with the values */
  def swap[K, V](implicit ev: <:<[T, (K, V)]): TypedPipe[(V, K)] =
    raiseTo[(K, V)].map(Swap())

  /** Just keep the values, or ._2 (if this type is a Tuple2) */
  def values[V](implicit ev: <:<[T, (Any, V)]): TypedPipe[V] =
    raiseTo[(Any, V)].map(GetValue())

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
    map(((), _)).hashLeftJoin(thatPipe.groupAll).values

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
   * These operations look like joins, but they do not force any communication
   * of the current TypedPipe. They are mapping operations where this pipe is streamed
   * through one item at a time.
   *
   * WARNING These behave semantically very differently than cogroup.
   * This is because we handle (K,V) tuples on the left as we see them.
   * The iterable on the right is over all elements with a matching key K, and it may be empty
   * if there are no values for this key K.
   */
  def hashCogroup[K, V, W, R](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
    TypedPipe.HashCoGroup(ev(this), smaller, joiner).withLine

  /** Do an inner-join without shuffling this TypedPipe, but replicating argument to all tasks */
  def hashJoin[K, V, W](smaller: HashJoinable[K, W])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, (V, W))] =
    hashCogroup[K, V, W, (V, W)](smaller)(Joiner.hashInner2)

  /** Do an leftjoin without shuffling this TypedPipe, but replicating argument to all tasks */
  def hashLeftJoin[K, V, W](smaller: HashJoinable[K, W])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, (V, Option[W]))] =
    hashCogroup[K, V, W, (V, Option[W])](smaller)(Joiner.hashLeft2)

  /**
   * For each element, do a map-side (hash) left join to look up a value
   */
  def hashLookup[K >: T, V](grouped: HashJoinable[K, V]): TypedPipe[(K, Option[V])] =
    map(WithConstant(()))
      .hashLeftJoin(grouped)
      .map(DropValue1())

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
  def sketch[K, V](reducers: Int,
    eps: Double = 1.0E-5, //272k width = 1MB per row
    delta: Double = 0.01, //5 rows (= 5 hashes)
    seed: Int = 12345)(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)],
      serialization: K => Array[Byte],
      ordering: Ordering[K]): Sketched[K, V] =
    Sketched(ev(this), reducers, delta, eps, seed)
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
