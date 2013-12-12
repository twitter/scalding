/*
Copyright 2012 Twitter, Inc.

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

import com.twitter.algebird.{Semigroup, MapAlgebra, Monoid, Ring, Aggregator}

import com.twitter.scalding.TupleConverter.{singleConverter, tuple2Converter, CTupleConverter, TupleEntryConverter}
import com.twitter.scalding.TupleSetter.{singleSetter, tup2Setter}

import com.twitter.scalding._

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.{Fields, Tuple => CTuple, TupleEntry}
import util.Random

/**
 * factory methods for TypedPipe, which is the typed representation of distributed lists in scalding.
 * This object is here rather than in the typed package because a lot of code was written using
 * the functions in the object, which we do not see how to hide with package object tricks.
 */
object TypedPipe extends Serializable {
  def from[T](pipe: Pipe, fields: Fields)(implicit conv: TupleConverter[T]): TypedPipe[T] =
    TypedPipeInst[T](pipe, fields, Converter(conv))

  def from[T](mappable: TypedSource[T])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    TypedPipeInst[T](mappable.read, mappable.sourceFields, Converter(mappable.converter))

  def from[T](iter: Iterable[T])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    IterablePipe[T](iter.view, flowDef, mode)

  /** Input must be a Pipe with exactly one Field */
  def fromSingleField[T](pipe: Pipe): TypedPipe[T] =
    TypedPipeInst[T](pipe, new Fields(0), Converter(singleConverter[T]))

  def empty(implicit flowDef: FlowDef, mode: Mode): TypedPipe[Nothing] =
    EmptyTypedPipe(flowDef, mode)
}

/** Think of a TypedPipe as a distributed unordered list that may or may not yet
 * have been materialized in memory or disk.
 *
 * Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
trait TypedPipe[+T] extends Serializable {

  // Implements a cross product.  The right side should be tiny
  def cross[U](tiny: TypedPipe[U]): TypedPipe[(T,U)]

  def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U]

  /** If you are going to create two branches or forks,
   * it may be more efficient to call this method first
   * which will create a node in the cascading graph.
   * Without this, both full branches of the fork will be
   * put into separate cascading pipes, which can, in some cases,
   * be slower.
   *
   * Ideally the planner would see this
   */
  def fork: TypedPipe[T]

  /** limit the output to at most count items.
   * useful for debugging, but probably that's about it.
   * The number may be less than count, and not sampled particular method
   */
  def limit(count: Int): TypedPipe[T]

  /** This does a sum of values WITHOUT triggering a shuffle.
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
  def sumByLocalKeys[K,V](implicit ev : T <:< (K,V), sg: Semigroup[V]): TypedPipe[(K,V)]

  def sample(percent : Double): TypedPipe[T]
  def sample(percent : Double, seed : Long): TypedPipe[T]

  /** Export back to a raw cascading Pipe. useful for interop with the scalding
   * Fields API or with Cascading code.
   */
  def toPipe[U >: T](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe

/////////////////////////////////////////////
//
// The following have default implementations in terms of the above
//
/////////////////////////////////////////////

  import Dsl._

  def ++[U >: T](other: TypedPipe[U]): TypedPipe[U] = other match {
    case EmptyTypedPipe(_,_) => this
    case IterablePipe(thatIter,_,_) if thatIter.isEmpty => this
    case _ => MergedTypedPipe(this, other)
  }

  /** Same as groupAll.aggregate.values
   */
  def aggregate[B,C](agg: Aggregator[T, B, C]): ValuePipe[C] =
    ComputedValue(groupAll.aggregate(agg).values)

  def collect[U](fn: PartialFunction[T, U]): TypedPipe[U] =
    filter(fn.isDefinedAt(_)).map(fn(_))

  // prints the current pipe to stdout
  def debug: TypedPipe[T] = map { t => println(t); t }

  /**
   * Returns the set of distinct elements in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinct method to work, the type in TypedPipe must have an Ordering.")
  def distinct(implicit ord: Ordering[_ >: T]): TypedPipe[T] = {
    // cast because Ordering is not contravariant, but should be (and this cast is safe)
    implicit val ordT: Ordering[T] = ord.asInstanceOf[Ordering[T]]
    map{ (_, ()) }.group.sum.keys
  }

  def either[R](that: TypedPipe[R]): TypedPipe[Either[T, R]] =
    map(Left(_)) ++ (that.map(Right(_)))

  /** Sometimes useful for implementing custom joins with groupBy + mapValueStream when you know
   * that the value/key can fit in memory. Beware.
   */
  def eitherValues[K,V,R](that: TypedPipe[(K, R)])(implicit ev: T <:< (K,V)): TypedPipe[(K, Either[V, R])] =
    mapValues { (v: V) => Left(v) } ++ (that.mapValues { (r: R) => Right(r) })

  def map[U](f: T => U): TypedPipe[U] = flatMap { t => Iterable(f(t)) }

  def mapValues[K, V, U](f : V => U)(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    map { t: T =>
      val (k, v) = t.asInstanceOf[(K, V)] //No need to capture ev and deal with serialization
      (k, f(v))
    }

  /** Keep only items satisfying a predicate
   */
  def filter(f: T => Boolean): TypedPipe[T] =
    flatMap { Iterable(_).filter(f) }

  /** flatten an Iterable */
  def flatten[U](implicit ev: T <:< TraversableOnce[U]): TypedPipe[U] =
    flatMap { _.asInstanceOf[TraversableOnce[U]] } // don't use ev which may not be serializable

  /** Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   */
  def forceToDisk: TypedPipe[T] =
    TypedPipe.fromSingleField(fork.toPipe(0).forceToDisk)

  def group[K,V](implicit ev : <:<[T,(K,V)], ord : Ordering[K]) : Grouped[K,V] =
    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.
    groupBy { (t : T) => t.asInstanceOf[(K,V)]._1 }(ord)
      .mapValues { (t : T) => t.asInstanceOf[(K,V)]._2 }

  def groupAll : Grouped[Unit,T] = groupBy(x => ()).withReducers(1)

  def groupBy[K](g : (T => K))(implicit ord : Ordering[K]) : Grouped[K,T] =
    Grouped(fork.map { t => (g(t), t) })

  /** Forces a shuffle by randomly assigning each item into one
   * of the partitions.
   *
   * This is for the case where you mappers take a long time, and
   * it is faster to shuffle them to more reducers and then operate.
   *
   * You probably want shard if you are just forcing a shuffle.
   */
  def groupRandomly(partitions: Int): Grouped[Int, T] = {
    // Make it lazy so all mappers get their own:
    lazy val rng = new java.util.Random()
    groupBy { _ => rng.nextInt(partitions) }
      .mapValues(identity(_)) // hack to get scalding to actually do the groupBy
      .withReducers(partitions)
  }
  /** Used to force a shuffle into a given size of nodes.
   * Only use this if your mappers are taking far longer than
   * the time to shuffle.
   */
  def shard(partitions: Int): TypedPipe[T] = groupRandomly(partitions).values

  /** Reasonably common shortcut for cases of associative/commutative reduction
   * returns a typed pipe with only one element.
   */
  def sum[U >: T](implicit plus: Semigroup[U]): ValuePipe[U] = ComputedValue(groupAll.sum[U].values)

  /** Reasonably common shortcut for cases of associative/commutative reduction by Key
   */
  def sumByKey[K,V](implicit ev: T<:<(K,V), ord: Ordering[K], plus: Semigroup[V]): TypedPipe[(K, V)] =
    group[K, V].sum[V]

  def unpackToPipe[U >: T](fieldNames: Fields)(implicit up: TupleUnpacker[U]): Pipe = {
    val setter = up.newSetter(fieldNames)
    toPipe[U](fieldNames)(setter)
  }

  /** Safely write to a TypedSink[T]. If you want to write to a Source (not a Sink)
   * you need to do something like: toPipe(fieldNames).write(dest)
   * @return a pipe equivalent to the current pipe.
   */
  def write(dest: TypedSink[T])
    (implicit flowDef : FlowDef, mode : Mode): TypedPipe[T] = {
    // Make sure that we don't render the whole pipeline twice:
    val res = fork
    dest.writeFrom(res.toPipe[T](dest.sinkFields)(dest.setter))
    res
  }

  def keys[K](implicit ev : <:<[T,(K,_)]) : TypedPipe[K] =
    // avoid capturing ev in the closure:
    map { t => t.asInstanceOf[(K, _)]._1 }

  // swap the keys with the values
  def swap[K,V](implicit ev: <:<[T,(K,V)]) : TypedPipe[(V,K)] = map { tup =>
    val (k,v) = tup.asInstanceOf[(K,V)]
    (v,k)
  }

  def values[V](implicit ev : <:<[T,(_,V)]) : TypedPipe[V] =
    // avoid capturing ev in the closure:
    map { t => t.asInstanceOf[(_, V)]._2 }

  def leftCross[V](p: ValuePipe[V]) : TypedPipe[(T, Option[V])] = {
    p match {
      case EmptyValue() => map { (_, None) }
      case LiteralValue(v) => map { (_, Some(v)) }
      case ComputedValue(pipe) => groupAll.hashLeftJoin(pipe.groupAll).values
    }
  }

  def mapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => V) : TypedPipe[V] =
    leftCross(value).map(t => f(t._1, t._2))

  def flatMapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => TraversableOnce[V]) : TypedPipe[V] =
    leftCross(value).flatMap(t => f(t._1, t._2))

  def filterWithValue[U](value: ValuePipe[U])(f: (T, Option[U]) => Boolean) : TypedPipe[T] =
    leftCross(value).filter(t => f(t._1, t._2)).map(_._1)
}


final case class EmptyTypedPipe(@transient fd: FlowDef, @transient mode: Mode) extends TypedPipe[Nothing] {
  import Dsl._

  override def aggregate[B, C](agg: Aggregator[Nothing, B, C]): ValuePipe[C] =
    EmptyValue()(fd, mode)

  // Cross product with empty is always empty.
  override def cross[U](tiny : TypedPipe[U]): TypedPipe[(Nothing,U)] =
    EmptyTypedPipe(fd, mode)

  override def distinct(implicit ord: Ordering[_ >: Nothing])  =
    this

  override def flatMap[U](f: Nothing => TraversableOnce[U]) =
    EmptyTypedPipe(fd, mode)

  override def fork: TypedPipe[Nothing] = this

  override def leftCross[V](p: ValuePipe[V]) =
    EmptyTypedPipe(fd, mode)

  /** limit the output to at most count items.
   * useful for debugging, but probably that's about it.
   * The number may be less than count, and not sampled particular method
   */
  override def limit(count: Int) = this

  override def sample(percent: Double) = this
  override def sample(percent: Double, seed: Long) = this

  // prints the current pipe to either stdout or stderr
  override def debug: TypedPipe[Nothing] = this

  override def ++[U >: Nothing](other : TypedPipe[U]): TypedPipe[U] = other

  override def toPipe[U >: Nothing](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe =
    IterableSource(Iterable.empty, fieldNames)(setter, singleConverter[U]).read(fd, mode)

  override def sum[U >: Nothing](implicit plus: Semigroup[U]): ValuePipe[U] =
    EmptyValue()(fd, mode)

  override def sumByLocalKeys[K,V](implicit ev : Nothing <:< (K,V), sg: Semigroup[V]) =
    EmptyValue()(fd, mode)
}

/** You should use a view here
 * If you avoid toPipe, this class is more efficient than IterableSource.
 */
final case class IterablePipe[T](iterable: Iterable[T],
  @transient fd: FlowDef,
  @transient mode: Mode) extends TypedPipe[T] {

  override def aggregate[B, C](agg: Aggregator[T, B, C]): ValuePipe[C] =
    Some(iterable)
      .filterNot(_.isEmpty)
      .map(it => LiteralValue(agg(it))(fd, mode))
      .getOrElse(EmptyValue()(fd, mode))

  override def ++[U >: T](other: TypedPipe[U]): TypedPipe[U] = other match {
    case IterablePipe(thatIter,_,_) => IterablePipe(iterable ++ thatIter, fd, mode)
    case EmptyTypedPipe(_,_) => this
    case _ if iterable.isEmpty => other
    case _ => MergedTypedPipe(this, other)
  }

  // Implements a cross product.
  override def cross[U](tiny : TypedPipe[U]) =
    tiny.flatMap { u => iterable.map { (_, u) } }

  override def filter(f: T => Boolean): TypedPipe[T] =
    IterablePipe(iterable.filter(f), fd, mode)

  override def flatMap[U](f: T => TraversableOnce[U]) =
    IterablePipe(iterable.flatMap(f), fd, mode)

  override def fork: TypedPipe[T] = this

  override def limit(count: Int): TypedPipe[T] = IterablePipe(iterable.take(count), fd, mode)

  private def defaultSeed: Long = System.identityHashCode(this) * 2654435761L ^ System.currentTimeMillis
  def sample(percent: Double): TypedPipe[T] = sample(percent, defaultSeed)
  def sample(percent: Double, seed: Long): TypedPipe[T] = {
    val rand = new Random(seed)
    IterablePipe(iterable.filter(_ => rand.nextDouble < percent), fd, mode)
  }

  override def map[U](f: T => U): TypedPipe[U] =
    IterablePipe(iterable.map(f), fd, mode)

  override def toPipe[U >: T](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe =
    IterableSource[U](iterable, fieldNames)(setter, singleConverter[U]).read(fd, mode)

  override def sum[U >: T](implicit plus: Semigroup[U]): ValuePipe[U] =
    Semigroup.sumOption[U](iterable).map(LiteralValue(_)(fd, mode))
      .getOrElse(EmptyValue()(fd, mode))

  override def sumByLocalKeys[K,V](implicit ev: T <:< (K,V), sg: Semigroup[V]) =
    IterablePipe(MapAlgebra.sumByKey(iterable.map(ev(_))), fd, mode)
}

/** This is an instance of a TypedPipe that wraps a cascading Pipe
 */
final case class TypedPipeInst[T](@transient inpipe: Pipe,
  fields: Fields,
  flatMapFn: FlatMapFn[T]) extends TypedPipe[T] {

  import Dsl._

  // The output pipe has a single item CTuple with an object of type T in position 0
  @transient protected lazy val pipe: Pipe = toPipe(0)(singleSetter[T])

  // Implements a cross product.  The right side should be tiny (< 100MB)
  override def cross[U](tiny : TypedPipe[U]): TypedPipe[(T,U)] = tiny match {
    case EmptyTypedPipe(fd, m) => EmptyTypedPipe(fd, m)
    case tpi@TypedPipeInst(_,_,_) =>
      val crossedPipe = pipe.rename(0 -> 't)
        .crossWithTiny(tpi.pipe.rename(0 -> 'u))
      TypedPipe.from(crossedPipe, ('t,'u))(tuple2Converter[T,U])
    case MergedTypedPipe(l, r) =>
      MergedTypedPipe(cross(l), cross(r))
    case IterablePipe(iter, _, _) =>
      flatMap { t => iter.map { (t, _) } }
  }

  // prints the current pipe to either stdout or stderr
  override def debug: TypedPipe[T] =
    TypedPipe.fromSingleField(pipe.debug)

  override def filter(f: T => Boolean): TypedPipe[T] =
    TypedPipeInst[T](inpipe, fields, flatMapFn.filter(f))

  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    TypedPipeInst[U](inpipe, fields, flatMapFn.flatMap(f))

  /** If you are going to create two branches or forks,
   * it may be more efficient to call this method first
   * which will create a node in the cascading graph.
   * Without this, both full branches of the fork will be
   * put into separate cascading.
   *
   * Ideally the planner would see this
   */
  override def fork: TypedPipe[T] =
    TypedPipe.fromSingleField(pipe)

  /** Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   */
  override lazy val forceToDisk: TypedPipe[T] =
    TypedPipe.fromSingleField(pipe.forceToDisk)

  /** limit the output to at most count items.
   * useful for debugging, but probably that's about it.
   * The number may be less than count, and not sampled particular method
   */
  override def limit(count: Int): TypedPipe[T] =
    TypedPipe.fromSingleField(pipe.limit(count))

  override def sample(percent: Double): TypedPipe[T] = TypedPipe.fromSingleField(pipe.sample(percent))
  override def sample(percent: Double, seed: Long): TypedPipe[T] = TypedPipe.fromSingleField(pipe.sample(percent, seed))

  override def map[U](f: T => U): TypedPipe[U] =
    TypedPipeInst[U](inpipe, fields, flatMapFn.map(f))

  override def sumByLocalKeys[K,V](implicit ev : T <:< (K,V), sg: Semigroup[V]): TypedPipe[(K,V)] = {
    val fields = ('key, 'value)
    val msr = new MapsideReduce(sg, 'key, 'value, None)(singleConverter[V], singleSetter[V])
    TypedPipe.from[(K,V)](
      map(_.asInstanceOf[(K,V)])
        .toPipe[(K, V)](fields).eachTo(fields -> fields) { _ => msr },
      fields)
  }
  /** This actually runs all the pure map functions in one Cascading Each
   * This approach is more efficient than untyped scalding because we
   * don't use TupleConverters/Setters after each map.
   */
  override def toPipe[U >: T](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe =
    inpipe.flatMapTo[TupleEntry, U](fields -> fieldNames)(flatMapFn)
}

final case class MergedTypedPipe[T](left: TypedPipe[T], right: TypedPipe[T]) extends TypedPipe[T] {
  import Dsl._

  // Implements a cross project.  The right side should be tiny
  def cross[U](tiny : TypedPipe[U]): TypedPipe[(T,U)] = tiny match {
    case EmptyTypedPipe(fd, m) => EmptyTypedPipe(fd, m)
    case _ => MergedTypedPipe(left.cross(tiny), right.cross(tiny))
  }

  // prints the current pipe to either stdout or stderr
  override def debug: TypedPipe[T] =
    MergedTypedPipe(left.debug, right.debug)

  override def filter(f: T => Boolean): TypedPipe[T] =
    MergedTypedPipe(left.filter(f), right.filter(f))

  def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    MergedTypedPipe(left.flatMap(f), right.flatMap(f))

  def limit(count: Int): TypedPipe[T] =
    TypedPipe.fromSingleField(fork.toPipe(0).limit(count))

  def sample(percent: Double): TypedPipe[T] =
    MergedTypedPipe(left.sample(percent), right.sample(percent))

  def sample(percent: Double, seed: Long): TypedPipe[T] =
    MergedTypedPipe(left.sample(percent, seed), right.sample(percent, seed))

  override def sumByLocalKeys[K,V](implicit ev : T <:< (K,V), sg: Semigroup[V]):
    TypedPipe[(K, V)] =
    MergedTypedPipe(left.sumByLocalKeys, right.sumByLocalKeys)

  override def map[U](f: T => U): TypedPipe[U] =
    MergedTypedPipe(left.map(f), right.map(f))

  override def fork: TypedPipe[T] =
    MergedTypedPipe(left.fork, right.fork)

  override def toPipe[U >: T](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe = {
    if(left == right) {
      //use map:
      left.flatMap {t => List(t, t)}.toPipe[U](fieldNames)
    }
    else {
      import RichPipe.assignName
      new cascading.pipe.Merge(assignName(left.toPipe[U](fieldNames)),
        assignName(right.toPipe[U](fieldNames)))
    }
  }
}

class TuplePipeJoinEnrichment[K, V](pipe: TypedPipe[(K, V)])(implicit ord: Ordering[K]) {
  def join[W](smaller : TypedPipe[(K, W)], reducers: Int = -1) : KeyedList[K, (V, W)] = pipe.group.withReducers(reducers).join(smaller.group)
  def leftJoin[W](smaller : TypedPipe[(K, W)], reducers: Int = -1) : KeyedList[K, (V, Option[W])] = pipe.group.withReducers(reducers).leftJoin(smaller.group)
  def rightJoin[W](smaller : TypedPipe[(K, W)], reducers: Int = -1) : KeyedList[K, (Option[V], W)] = pipe.group.withReducers(reducers).rightJoin(smaller.group)
  def outerJoin[W](smaller : TypedPipe[(K, W)], reducers: Int = -1) : KeyedList[K, (Option[V], Option[W])] = pipe.group.withReducers(reducers).outerJoin(smaller.group)
}

class MappablePipeJoinEnrichment[T](pipe: TypedPipe[T]) {
  def joinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K), reducers: Int = -1)(implicit ord: Ordering[K]) : KeyedList[K, (T, U)] = pipe.groupBy(g).withReducers(reducers).join(smaller.groupBy(h))
  def leftJoinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K), reducers: Int = -1)(implicit ord: Ordering[K]) : KeyedList[K, (T, Option[U])] = pipe.groupBy(g).withReducers(reducers).leftJoin(smaller.groupBy(h))
  def rightJoinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K), reducers: Int = -1)(implicit ord: Ordering[K]) : KeyedList[K, (Option[T], U)] = pipe.groupBy(g).withReducers(reducers).rightJoin(smaller.groupBy(h))
  def outerJoinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K), reducers: Int = -1)(implicit ord: Ordering[K]) : KeyedList[K, (Option[T], Option[U])] = pipe.groupBy(g).withReducers(reducers).outerJoin(smaller.groupBy(h))
}

object Syntax {
  implicit def joinOnTuplePipe[K, V](p: TypedPipe[(K, V)])(implicit ord: Ordering[K]) : TuplePipeJoinEnrichment[K, V] = new TuplePipeJoinEnrichment(p)
  implicit def joinOnMappablePipe[T](p: TypedPipe[T]) : MappablePipeJoinEnrichment[T] = new MappablePipeJoinEnrichment(p)
}
