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

import com.twitter.algebird.{Semigroup, Ring, Aggregator}

import com.twitter.scalding.TupleConverter.{singleConverter, tuple2Converter, CTupleConverter, TupleEntryConverter}
import com.twitter.scalding.TupleSetter.{singleSetter, tup2Setter}

import com.twitter.scalding._

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.{Fields, Tuple => CTuple, TupleEntry}
/**
 * factory methods for TypedPipe, which is the typed representation of distributed lists in scalding.
 * This object is here rather than in the typed package because a lot of code was written using
 * the functions in the object, which we do not see how to hide with package object tricks.
 */
object TypedPipe extends Serializable {
  def from[T](pipe: Pipe, fields: Fields)(implicit conv: TupleConverter[T]): TypedPipe[T] =
    new TypedPipe[T](pipe, fields, {te => Some(conv(te))})

  def from[T](mappable: TypedSource[T])(implicit flowDef: FlowDef, mode: Mode) =
    new TypedPipe[T](mappable.read, mappable.sourceFields, {te => Some(mappable.converter(te))})
}

/** Think of a TypedPipe as a distributed unordered list that may or may not yet
 * have been materialized in memory or disk.
 *
 * Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
class TypedPipe[+T] private (inpipe : Pipe, fields : Fields, flatMapFn : (TupleEntry) => TraversableOnce[T])
  extends Serializable {
  import Dsl._

  /** This actually runs all the pure map functions in one Cascading Each
   * This approach is more efficient than untyped scalding because we
   * don't use TupleConverters/Setters after each map.
   * The output pipe has a single item CTuple with an object of type T in position 0
   */
  protected lazy val pipe: Pipe =
    inpipe.flatMapTo(fields -> 0)(flatMapFn)(TupleEntryConverter, singleSetter[T])

  /** Same as groupAll.aggregate.values
   */
  def aggregate[B,C](agg: Aggregator[T,B,C]): TypedPipe[C] = groupAll.aggregate(agg).values

  // Implements a cross project.  The right side should be tiny
  def cross[U](tiny : TypedPipe[U]) : TypedPipe[(T,U)] = {
    val crossedPipe = pipe.rename(0 -> 't)
      .crossWithTiny(tiny.pipe.rename(0 -> 'u))
    TypedPipe.from(crossedPipe, ('t,'u))(tuple2Converter[T,U])
  }

  def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    new TypedPipe[U](inpipe, fields, { te => flatMapFn(te).flatMap(f) })

  /** limit the output to at most count items.
   * useful for debugging, but probably that's about it.
   * The number may be less than count, and not sampled particular method
   */
  def limit(count: Int): TypedPipe[T] =
    TypedPipe.from[T](pipe.limit(count), 0)

  // prints the current pipe to either stdout or stderr
  def debug: TypedPipe[T] =
    TypedPipe.from[T](this.pipe.debug, 0)

  /**
   * Returns the set of distinct elements in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinct method to work, the type in TypedPipe must have an Ordering.")
  def distinct(implicit ord: Ordering[_ >: T]): TypedPipe[T] = {
    // cast because Ordering is not contravariant, but should be (and this cast is safe)
    implicit val ordT: Ordering[T] = ord.asInstanceOf[Ordering[T]]
    map{ (_, ()) }.group.sum.keys
  }

  def map[U](f: T => U): TypedPipe[U] =
    new TypedPipe[U](inpipe, fields, { te => flatMapFn(te).map(f) })

  def mapValues[K, V, U](f : V => U)(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    map { t: T =>
      val (k, v) = t.asInstanceOf[(K, V)] //No need to capture ev and deal with serialization
      (k, f(v))
    }

  /** Keep only items satisfying a predicate
   */
  def filter(f: T => Boolean): TypedPipe[T] = {
    new TypedPipe[T](inpipe, fields, { te => flatMapFn(te).filter(f) })
  }
  /** flatten an Iterable */
  def flatten[U](implicit ev: T <:< TraversableOnce[U]): TypedPipe[U] =
    flatMap { _.asInstanceOf[TraversableOnce[U]] } // don't use ev which may not be serializable

  /** If you are going to create two branches or forks,
   * it may be more efficient to call this method first
   * which will create a node in the cascading graph.
   * Without this, both full branches of the fork will be
   * put into separate cascading.
   *
   * Ideally the planner would see this
   */
  def fork: TypedPipe[T] = TypedPipe.from(pipe, 0)(singleConverter[T])

  /** Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   */
  lazy val forceToDisk: TypedPipe[T] = TypedPipe.from(pipe.forceToDisk, 0)(singleConverter[T])

  def group[K,V](implicit ev : <:<[T,(K,V)], ord : Ordering[K]) : Grouped[K,V] =
    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.
    groupBy { (t : T) => t.asInstanceOf[(K,V)]._1 }(ord)
      .mapValues { (t : T) => t.asInstanceOf[(K,V)]._2 }

  lazy val groupAll : Grouped[Unit,T] = groupBy(x => ()).withReducers(1)

  def groupBy[K](g : (T => K))(implicit ord : Ordering[K]) : Grouped[K,T] = {
    // due to type erasure, I'm fairly sure this is not using the primitive TupleGetters
    // Note, lazy val pipe returns a single count tuple with an object of type T in position 0
    val gpipe = pipe.mapTo[T,(K,T)](0 -> ('key, 'value)) { (t : T) => (g(t), t)}(singleConverter[T], TupleSetter.tup2Setter[(K,T)])
    Grouped.fromKVPipe[K,T](gpipe, ord)
  }
  def ++[U >: T](other : TypedPipe[U]) : TypedPipe[U] =
    TypedPipe.from(pipe ++ other.pipe, 0)(singleConverter[U])

  /** Reasonably common shortcut for cases of associative/commutative reduction
   * returns a typed pipe with only one element.
   */
  def sum[U >: T](implicit plus: Semigroup[U]): TypedPipe[U] = groupAll.sum[U].values

  def toPipe[U >: T](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe =
    inpipe.flatMapTo[TupleEntry, U](fields -> fieldNames)(flatMapFn)

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

    val toWrite = pipe.mapTo[T,T](0 -> dest.sinkFields)(identity _)(singleConverter[T], dest.setter[T])
    dest.writeFrom(toWrite)
    // To avoid needing the inverse TupleConverter for dest.setter, just use the pipe before setting:
    TypedPipe.from[T](pipe, 0)
  }

  def keys[K](implicit ev : <:<[T,(K,_)]) : TypedPipe[K] = map { _._1 }

  // swap the keys with the values
  def swap[K,V](implicit ev: <:<[T,(K,V)]) : TypedPipe[(V,K)] = map { tup =>
    val (k,v) = tup.asInstanceOf[(K,V)]
    (v,k)
  }

  def values[V](implicit ev : <:<[T,(_,V)]) : TypedPipe[V] = map { _._2 }
}

class TuplePipeJoinEnrichment[K, V](pipe: TypedPipe[(K, V)])(implicit ord: Ordering[K]) {
  def join[W](smaller : TypedPipe[(K, W)]) : KeyedList[K, (V, W)] = pipe.group.join(smaller.group)
  def leftJoin[W](smaller : TypedPipe[(K, W)]) : KeyedList[K, (V, Option[W])] = pipe.group.leftJoin(smaller.group)
  def rightJoin[W](smaller : TypedPipe[(K, W)]) : KeyedList[K, (Option[V], W)] = pipe.group.rightJoin(smaller.group)
  def outerJoin[W](smaller : TypedPipe[(K, W)]) : KeyedList[K, (Option[V], Option[W])] = pipe.group.outerJoin(smaller.group)
}

class MappablePipeJoinEnrichment[T](pipe: TypedPipe[T]) {
  def joinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K))(implicit ord: Ordering[K]) : KeyedList[K, (T, U)] = pipe.groupBy(g).join(smaller.groupBy(h))
  def leftJoinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K))(implicit ord: Ordering[K]) : KeyedList[K, (T, Option[U])] = pipe.groupBy(g).leftJoin(smaller.groupBy(h))
  def rightJoinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K))(implicit ord: Ordering[K]) : KeyedList[K, (Option[T], U)] = pipe.groupBy(g).rightJoin(smaller.groupBy(h))
  def outerJoinBy[K, U](smaller : TypedPipe[U])(g : (T => K), h : (U => K))(implicit ord: Ordering[K]) : KeyedList[K, (Option[T], Option[U])] = pipe.groupBy(g).outerJoin(smaller.groupBy(h))
}

object Syntax {
  implicit def joinOnTuplePipe[K, V](p: TypedPipe[(K, V)])(implicit ord: Ordering[K]) : TuplePipeJoinEnrichment[K, V] = new TuplePipeJoinEnrichment(p)
  implicit def joinOnMappablePipe[T](p: TypedPipe[T]) : MappablePipeJoinEnrichment[T] = new MappablePipeJoinEnrichment(p)
}