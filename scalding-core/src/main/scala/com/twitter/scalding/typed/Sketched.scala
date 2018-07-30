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

import com.twitter.algebird.{ Bytes, CMS, Batched }
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering._
import com.twitter.scalding.serialization.{ OrderedSerialization, OrderedSerialization2 }
import com.twitter.algebird.CMSMonoid

// This was a bad design choice, we should have just put these in the CMSHasher object

/**
 * This class is generally only created by users
 * with the TypedPipe.sketch method
 */
case class Sketched[K, V](pipe: TypedPipe[(K, V)],
  numReducers: Int,
  delta: Double,
  eps: Double,
  seed: Int)(implicit val serialization: K => Array[Byte],
    ordering: Ordering[K])
  extends MustHaveReducers {

  def reducers = Some(numReducers)

  lazy val sketch: TypedPipe[CMS[Bytes]] = {
    // don't close over Sketched
    val localSer = serialization
    val (leps, ldelta, lseed) = (eps, delta, seed)
    lazy implicit val cms: CMSMonoid[Bytes] = CMS.monoid[Bytes](leps, ldelta, lseed)

    // every 10k items, compact into a CMS to prevent very slow mappers
    lazy implicit val batchedSG: com.twitter.algebird.Semigroup[Batched[CMS[Bytes]]] = Batched.compactingSemigroup[CMS[Bytes]](10000)

    pipe
      .map { case (k, _) => ((), Batched(cms.create(Bytes(localSer(k))))) }
      .sumByLocalKeys
      .map {
        case (_, batched) => batched.sum
      } // remove the Batched before going to the reducers
      .groupAll
      .sum
      .values
      .forceToDisk // make sure we materialize when we have 1 item
  }

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
  def join[V2](right: TypedPipe[(K, V2)]): SketchJoined[K, V, V2, (V, V2)] =
    cogroup(right)(Joiner.hashInner2)
  /**
   * Does a logical left join but replicates the heavy keys of the left hand side
   * across the reducers
   */
  def leftJoin[V2](right: TypedPipe[(K, V2)]): SketchJoined[K, V, V2, (V, Option[V2])] =
    cogroup(right)(Joiner.hashLeft2)
}

case class SketchJoined[K: Ordering, V, V2, R](left: Sketched[K, V],
  right: TypedPipe[(K, V2)],
  numReducers: Int)(joiner: (K, V, Iterable[V2]) => Iterator[R])
  extends MustHaveReducers {

  def reducers = Some(numReducers)

  //the most of any one reducer we want to try to take up with a single key
  private val maxReducerFraction = 0.1

  private def flatMapWithReplicas[W](pipe: TypedPipe[(K, W)])(fn: Int => Iterable[Int]) = {
    // don't close over Sketched
    val localSer = left.serialization
    val localNumReducers = numReducers
    val localMaxReducerFraction = maxReducerFraction

    pipe.cross(left.sketch).flatMap{
      case ((k, w), cms) =>
        val maxPerReducer = ((cms.totalCount * localMaxReducerFraction) / localNumReducers) + 1
        val maxReplicas = (cms.frequency(Bytes(localSer(k))).estimate.toDouble / maxPerReducer)
        //if the frequency is 0, maxReplicas.ceil will be 0 so we will filter out this key entirely
        //if it's < maxPerReducer, the ceil will round maxReplicas up to 1 to ensure we still see it
        val replicas = fn(maxReplicas.ceil.toInt.min(localNumReducers))
        replicas.map{ i => (i, k) -> w }
    }
  }

  val toTypedPipe: TypedPipe[(K, R)] = {
    lazy val rand = new scala.util.Random(left.seed)
    val lhs = flatMapWithReplicas(left.pipe){ n => (rand.nextInt(n) + 1) :: Nil }
    val rhs = flatMapWithReplicas(right){ n => 1.to(n) }

    lhs
      .group
      .cogroup(rhs.group){ (k, itv, itu) => itv.flatMap{ v => joiner(k._2, v, itu) } }
      .withReducers(numReducers)
      .map{ case ((r, k), v) => (k, v) }
  }

  private implicit def intKeyOrd: Ordering[(Int, K)] = {
    val kord = implicitly[Ordering[K]]

    kord match {
      case kos: OrderedSerialization[_] => new OrderedSerialization2(ordSer[Int], kos.asInstanceOf[OrderedSerialization[K]])
      case _ => Ordering.Tuple2[Int, K]
    }
  }

}

object SketchJoined {
  implicit def toTypedPipe[K, V, V2, R](joined: SketchJoined[K, V, V2, R]): TypedPipe[(K, R)] = joined.toTypedPipe
  implicit def toTypedPipeKeyed[K, V, V2, R](joined: SketchJoined[K, V, V2, R]): TypedPipe.Keyed[K, R] =
    new TypedPipe.Keyed(joined.toTypedPipe)
}
