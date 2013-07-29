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

import com.twitter.scalding._

/** Represents sharded lists of items of type T
 */
trait KeyedList[+K,+T] {
  // These are the fundamental operations
  def toTypedPipe : TypedPipe[(K,T)]
  /** Operate on a Stream[T] of all the values for each key at one time.
   * Avoid accumulating the whole list in memory if you can.  Prefer reduce.
   */
  def mapValueStream[V](smfn : Iterator[T] => Iterator[V]) : KeyedList[K,V]

  ///////////
  /// The below are all implemented in terms of the above:
  ///////////

  /** Use Algebird Aggregator to do the reduction
   */
  def aggregate[B,C](agg: Aggregator[T,B,C]): TypedPipe[(K,C)] =
    mapValues(agg.prepare _)
      .reduce(agg.reduce _)
      .map { kv => (kv._1, agg.present(kv._2)) }

  /** This is a special case of mapValueStream, but can be optimized because it doesn't need
   * all the values for a given key at once.  An unoptimized implementation is:
   * mapValueStream { _.map { fn } }
   * but for Grouped we can avoid resorting to mapValueStream
   */
  def mapValues[V](fn : T => V) : KeyedList[K,V] = mapValueStream { _.map { fn } }
  /** reduce with fn which must be associative and commutative.
   * Like the above this can be optimized in some Grouped cases.
   * If you don't have a commutative operator, use reduceLeft
   */
  def reduce[U >: T](fn : (U,U) => U) : TypedPipe[(K,U)] = reduceLeft(fn)

  // The rest of these methods are derived from above
  def sum[U >: T](implicit sg: Semigroup[U]) = reduce(sg.plus)
  def product[U >: T](implicit ring : Ring[U]) = reduce(ring.times)
  def count(fn : T => Boolean) : TypedPipe[(K,Long)] = {
    mapValues { t => if (fn(t)) 1L else 0L }.sum
  }
  def forall(fn : T => Boolean) : TypedPipe[(K,Boolean)] = {
    mapValues { fn(_) }.product
  }

  /**
   * Selects all elements except first n ones.
   */
  def drop(n: Int) : KeyedList[K, T] = {
    mapValueStream { _.drop(n) }
  }

  /**
   * Drops longest prefix of elements that satisfy the given predicate.
   */
  def dropWhile(p: (T) => Boolean): KeyedList[K, T] = {
     mapValueStream {_.dropWhile(p)}
  }

  /**
   * Selects first n elements.
   */
  def take(n: Int) : KeyedList[K, T] = {
    mapValueStream {_.take(n)}
  }

  /**
   * Takes longest prefix of elements that satisfy the given predicate.
   */
  def takeWhile(p: (T) => Boolean) : KeyedList[K, T] = {
    mapValueStream {_.takeWhile(p)}
  }

  def foldLeft[B](z : B)(fn : (B,T) => B) : TypedPipe[(K,B)] = {
    mapValueStream { stream => Iterator(stream.foldLeft(z)(fn)) }
      .toTypedPipe
  }
  def scanLeft[B](z : B)(fn : (B,T) => B) : KeyedList[K,B] = {
    // Get the implicit conversion for scala 2.8 to have scanLeft on an iterator:
    import Dsl._
    mapValueStream { _.scanLeft(z)(fn) }
  }
  // Similar to reduce but always on the reduce-side (never optimized to mapside),
  // and named for the scala function. fn need not be associative and/or commutative.
  // Makes sense when you want to reduce, but in a particular sorted order.
  // the old value comes in on the left.
  def reduceLeft[U >: T](fn : (U,U) => U) : TypedPipe[(K,U)] = {
    mapValueStream[U] { stream =>
      if (stream.isEmpty) {
        // We have to guard this case, as cascading seems to give empty streams on occasions
        Iterator.empty
      }
      else {
        Iterator(stream.reduceLeft(fn))
      }
    }
    .toTypedPipe
  }
  def size : TypedPipe[(K,Long)] = mapValues { x => 1L }.sum
  def toList : TypedPipe[(K,List[T])] = mapValues { List(_) }.sum
  // Note that toSet needs to be parameterized even though toList does not.
  // This is because List is covariant in its type parameter in the scala API,
  // but Set is invariant.  See:
  // http://stackoverflow.com/questions/676615/why-is-scalas-immutable-set-not-covariant-in-its-type
  def toSet[U >: T] : TypedPipe[(K,Set[U])] = mapValues { Set[U](_) }.sum
  def max[B >: T](implicit cmp : Ordering[B]) : TypedPipe[(K,T)] = {
    asInstanceOf[KeyedList[K,B]].reduce(cmp.max).asInstanceOf[TypedPipe[(K,T)]]
  }
  def maxBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TypedPipe[(K,T)] =
    reduce(Ordering.by(fn).max)

  def min[B >: T](implicit cmp : Ordering[B]) : TypedPipe[(K,T)] = {
    asInstanceOf[KeyedList[K,B]].reduce(cmp.min).asInstanceOf[TypedPipe[(K,T)]]
  }
  def minBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TypedPipe[(K,T)] =
    reduce(Ordering.by(fn).min)

  def keys : TypedPipe[K] = toTypedPipe.keys
  def values : TypedPipe[T] = toTypedPipe.values
}
