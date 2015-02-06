/*
Copyright 2014 Twitter, Inc.

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

import com.twitter.algebird.{ CMS, CMSHasher }

object Sketched {

  // TODO: there are more efficient orderings we could use here if this turns
  // out to be a bottleneck, and this should actually never end up gettings used.
  // We may be able to remove this after some refactoring in Algebird.
  implicit val byteArrayOrdering = Ordering.by((_: Array[Byte]).toIterable)

  /**
   * This is based on the CMSHasherBigInt found in algebird (see docs for in depth explanation):
   * https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/CountMinSketch.scala#L1086
   *
   * TODO: We need to move this hasher to CMSHasherImplicits in algebird:
   * https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/CountMinSketch.scala#L1054
   * See: https://github.com/twitter/scalding/issues/1177
   */
  implicit object CMSHasherByteArray extends CMSHasher[Array[Byte]] {
    override def hash(a: Int, b: Int, width: Int)(x: Array[Byte]): Int = {
      val hash: Int = scala.util.hashing.MurmurHash3.arrayHash(x, a)
      // We only want positive integers for the subsequent modulo.  This method mimics Java's Hashtable
      // implementation.  The Java code uses `0x7FFFFFFF` for the bit-wise AND, which is equal to Int.MaxValue.
      val positiveHash = hash & Int.MaxValue
      positiveHash % width
    }
  }
}

/**
 * This class is generally only created by users
 * with the TypedPipe.sketch method
 */
case class Sketched[K, V](pipe: TypedPipe[(K, V)],
  numReducers: Int,
  delta: Double,
  eps: Double,
  seed: Int)(implicit serialization: K => Array[Byte],
    ordering: Ordering[K])
  extends HasReducers {
  import Sketched._

  def serialize(k: K): Array[Byte] = serialization(k)

  val reducers = Some(numReducers)

  private lazy implicit val cms = CMS.monoid[Array[Byte]](eps, delta, seed)
  lazy val sketch: TypedPipe[CMS[Array[Byte]]] =
    pipe
      .map { case (k, v) => cms.create(serialization(k)) }
      .groupAll
      .sum
      .values
      .forceToDisk

  /**
   * Like a hashJoin, this joiner does not see all the values V at one time, only one at a time.
   * This is sufficient to implement join and leftJoin
   */
  def cogroup[V2, R](right: TypedPipe[(K, V2)])(joiner: (K, V, Iterable[V2]) => Iterator[R]): SketchJoined[K, V, V2, R] =
    new SketchJoined(this, right, numReducers)(joiner)

  /**
   * Does a logical inner join but replicates the heavy keys of the left hand side
   * across the reducers
   */
  def join[V2](right: TypedPipe[(K, V2)]) = cogroup(right)(Joiner.hashInner2)
  /**
   * Does a logical left join but replicates the heavy keys of the left hand side
   * across the reducers
   */
  def leftJoin[V2](right: TypedPipe[(K, V2)]) = cogroup(right)(Joiner.hashLeft2)
}

case class SketchJoined[K: Ordering, V, V2, R](left: Sketched[K, V],
  right: TypedPipe[(K, V2)],
  numReducers: Int)(joiner: (K, V, Iterable[V2]) => Iterator[R])
  extends HasReducers {

  val reducers = Some(numReducers)

  //the most of any one reducer we want to try to take up with a single key
  private val maxReducerFraction = 0.1

  private def flatMapWithReplicas[W](pipe: TypedPipe[(K, W)])(fn: Int => Iterable[Int]) =
    pipe.cross(left.sketch).flatMap{
      case (v, cms) =>
        val maxPerReducer = (cms.totalCount / numReducers) * maxReducerFraction + 1
        val maxReplicas = (cms.frequency(left.serialize(v._1)).estimate.toDouble / maxPerReducer)

        //if the frequency is 0, maxReplicas.ceil will be 0 so we will filter out this key entirely
        //if it's < maxPerReducer, the ceil will round maxReplicas up to 1 to ensure we still see it
        val replicas = fn(maxReplicas.ceil.toInt.min(numReducers))
        replicas.map{ i => (i, v._1) -> v._2 }
    }

  lazy val toTypedPipe: TypedPipe[(K, R)] = {
    lazy val rand = new scala.util.Random(left.seed)
    val lhs = flatMapWithReplicas(left.pipe){ n => Some(rand.nextInt(n) + 1) }
    val rhs = flatMapWithReplicas(right){ n => 1.to(n) }

    lhs
      .group
      .cogroup(rhs.group){ (k, itv, itu) => itv.flatMap{ v => joiner(k._2, v, itu) } }
      .withReducers(numReducers)
      .map{ case ((r, k), v) => (k, v) }
  }
}

object SketchJoined {
  implicit def toTypedPipe[K, V, V2, R](joined: SketchJoined[K, V, V2, R]): TypedPipe[(K, R)] = joined.toTypedPipe
}
