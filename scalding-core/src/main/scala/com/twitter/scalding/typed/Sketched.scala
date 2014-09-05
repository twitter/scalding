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

import com.twitter.algebird.{ CMS, MurmurHash128 }

case class Sketched[K, V](pipe: TypedPipe[(K, V)],
  numReducers: Int,
  delta: Double,
  eps: Double,
  seed: Int)(implicit serialization: K => Array[Byte],
    ordering: Ordering[K])
  extends HasReducers {

  val reducers = Some(numReducers)

  private lazy val murmurHash = MurmurHash128(seed)
  def hash(key: K): Long = murmurHash(serialization(key))._1

  private lazy implicit val cms = CMS.monoid(eps, delta, seed)
  lazy val sketch: TypedPipe[CMS] =
    pipe
      .map{ kv => cms.create(hash(kv._1)) }
      .groupAll
      .sum
      .values
      .forceToDisk

  def cogroup[V2, R](right: TypedPipe[(K, V2)])(joiner: (K, V, Iterable[V2]) => Iterator[R]): SketchJoined[K, V, V2, R] =
    new SketchJoined(this, right, numReducers)(joiner)

  def join[V2](right: TypedPipe[(K, V2)]) = cogroup(right)(Joiner.hashInner2)
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
        val maxReplicas = (cms.frequency(left.hash(v._1)).estimate.toDouble / maxPerReducer)

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
