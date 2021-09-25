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

import org.scalatest.WordSpec

import com.twitter.scalding.typed.FlattenGroup._

class MultiJoinTest extends WordSpec {

  def addKeys[V](t: Seq[V]): Seq[(Int, V)] = t.iterator.zipWithIndex.map { case (v, k) => (k, v) }.toSeq

  val doubles = TypedPipe.from(addKeys(List(1.0D, 2.0D, 3.0D)))
  val longs = TypedPipe.from(addKeys(List(10L, 20L, 30L)))
  val strings = TypedPipe.from(addKeys(List("one", "two", "three")))
  val sets = TypedPipe.from(addKeys(List(Set(1), Set(2), Set(3))))
  val maps = TypedPipe.from(addKeys(List(Map(1 -> 1), Map(2 -> 2), Map(3 -> 3))))

  val joined = doubles.join(longs).join(strings).join(sets).join(maps)
  val leftJoined = doubles.leftJoin(longs).leftJoin(strings).leftJoin(sets).leftJoin(maps)
  val outerJoined = doubles.outerJoin(longs).outerJoin(strings).outerJoin(sets).outerJoin(maps)

  // note that these tests are essentially compile-time tests, all
  // we are testing is that this compiles

  "The flatten methods" should {
    "actually match the outputs of joins" in {

      val joinedFlat: CoGrouped[Int, (Double, Long, String, Set[Int], Map[Int, Int])] =
        joined.mapValues { x => flattenNestedTuple(x) }

      val leftJoinedFlat: CoGrouped[Int, (Double, Option[Long], Option[String], Option[Set[Int]], Option[Map[Int, Int]])] =
        leftJoined.mapValues { x => flattenNestedTuple(x) }

      val outerJoinedFlat: CoGrouped[Int, (Option[Double], Option[Long], Option[String], Option[Set[Int]], Option[Map[Int, Int]])] =
        outerJoined.mapValues { x => flattenNestedOptionTuple(x) }
    }

    "Have implicit flattenValueTuple methods for low arity" in {

      val joinedFlat: CoGrouped[Int, (Double, Long, String, Set[Int], Map[Int, Int])] =
        joined.flattenValueTuple

      val leftJoinedFlat: CoGrouped[Int, (Double, Option[Long], Option[String], Option[Set[Int]], Option[Map[Int, Int]])] =
        leftJoined.flattenValueTuple

      val outerJoinedFlat: CoGrouped[Int, (Option[Double], Option[Long], Option[String], Option[Set[Int]], Option[Map[Int, Int]])] =
        outerJoined.flattenValueTuple
    }

  }
}