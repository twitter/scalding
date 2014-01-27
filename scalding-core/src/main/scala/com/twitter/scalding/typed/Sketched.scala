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

import com.twitter.algebird.{CMS,MurmurHash128}

case class Sketched[K,V]
  (pipe: TypedPipe[(K,V)],
  delta: Double,
  eps: Double,
  seed: Int,
  reducers: Option[Int])
  (implicit serialization: K => Array[Byte],
   ordering: Ordering[K])
    extends WithReducers[Sketched[K,V]] {

  def withReducers(n: Int) = Sketched(pipe, delta, eps, seed, Some(n))

  private lazy val murmurHash = MurmurHash128(seed)
  def hash(key: K) = murmurHash(serialization(key))._1

  private lazy implicit val cms = CMS.monoid(eps, delta, seed)
  lazy val sketch = pipe.map{kv => cms.create(hash(kv._1))}.sum

  def cogroup[V2,R](right: TypedPipe[(K,V2)])(joiner: (K, V, Iterable[V2]) => Iterator[R]) =
    new SketchJoined(this, right, reducers)(joiner)

  def join[V2](right: TypedPipe[(K,V2)]) = cogroup(right)(Joiner.hashInner2)
  def leftJoin[V2](right: TypedPipe[(K,V2)]) = cogroup(right)(Joiner.hashLeft2)
}

case class SketchJoined[K:Ordering,V,V2,R]
  (left: Sketched[K,V],
   right: TypedPipe[(K,V2)],
   reducers: Option[Int])
  (joiner: (K, V, Iterable[V2]) => Iterator[R])
    extends WithReducers[SketchJoined[K,V,V2,R]] {

  private lazy val numReducers = reducers.getOrElse(sys.error("Must specify number of reducers"))
  def withReducers(n: Int) = SketchJoined(left, right, Some(n))(joiner)

  //the most of any one reducer we want to try to take up with a single key
  val maxReducerFraction = 0.1

  private def flatMapWithReplicas[V](pipe: TypedPipe[(K,V)])(fn: Int => Iterable[Int]) =
    pipe.flatMapWithValue(left.sketch){(v,sketchOpt) =>
      sketchOpt.toList.flatMap{cms =>
        val maxPerReducer = (cms.totalCount / numReducers) * maxReducerFraction + 1
        val maxReplicas = (cms.frequency(left.hash(v._1)).estimate.toDouble / maxPerReducer)
        val replicas = fn(maxReplicas.ceil.toInt.min(numReducers))
        replicas.toList.map{i => (i,v._1) -> v._2}
      }
    }

  lazy val toTypedPipe = {
    lazy val rand = new scala.util.Random(left.seed)
    val lhs = flatMapWithReplicas(left.pipe){n => Some(rand.nextInt(n) + 1)}
    val rhs = flatMapWithReplicas(right){n => 1.to(n)}

    lhs
      .group
      .cogroup(rhs.group){(k,itv,itu) => itv.flatMap{v => joiner(k._2,v,itu)}}
      .withReducers(numReducers)
      .map{case ((r,k),v) => (k,v)}
  }
}

object SketchJoined {
  implicit def toTypedPipe[K,V, V2, R]
    (joined: SketchJoined[K, V, V2, R]): TypedPipe[(K, R)] = joined.toTypedPipe
}
