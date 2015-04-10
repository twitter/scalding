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

package com.twitter.scalding.graph

import org.scalacheck.Prop._
import org.scalacheck.{ Arbitrary, Gen, Properties }

/**
 * This tests the HMap. We use the type system to
 * prove the types are correct and don't (yet?) engage
 * in the problem of higher kinded Arbitraries.
 */
object HMapTests extends Properties("HMap") {
  case class Key[T](key: Int)
  case class Value[T](value: Int)

  implicit def keyGen: Gen[Key[Int]] = Gen.choose(Int.MinValue, Int.MaxValue).map(Key(_))
  implicit def valGen: Gen[Value[Int]] = Gen.choose(Int.MinValue, Int.MaxValue).map(Value(_))

  def zip[T, U](g: Gen[T], h: Gen[U]): Gen[(T, U)] = for {
    a <- g
    b <- h
  } yield (a, b)

  implicit def hmapGen: Gen[HMap[Key, Value]] =
    Gen.listOf(zip(keyGen, valGen)).map { list =>
      list.foldLeft(HMap.empty[Key, Value]) { (hm, kv) =>
        hm + kv
      }
    }

  implicit def arb[T](implicit g: Gen[T]): Arbitrary[T] = Arbitrary(g)

  property("adding a pair works") = forAll { (hmap: HMap[Key, Value], k: Key[Int], v: Value[Int]) =>
    val initContains = hmap.contains(k)
    val added = hmap + (k -> v)
    // Adding puts the item in, and does not change the initial
    (added.get(k) == Some(v)) &&
      (initContains == hmap.contains(k)) &&
      (initContains == hmap.get(k).isDefined)
  }
  property("removing a key works") = forAll { (hmap: HMap[Key, Value], k: Key[Int]) =>
    val initContains = hmap.get(k).isDefined
    val next = hmap - k
    // Adding puts the item in, and does not change the initial
    (!next.contains(k)) &&
      (initContains == hmap.contains(k)) &&
      (next.get(k) == None)
  }

  property("keysOf works") = forAll { (hmap: HMap[Key, Value], k: Key[Int], v: Value[Int]) =>
    val initKeys = hmap.keysOf(v)
    val added = hmap + (k -> v)
    val finalKeys = added.keysOf(v)
    val sizeIsConsistent = (finalKeys -- initKeys).size match {
      case 0 => hmap.contains(k) // initially present
      case 1 => !hmap.contains(k) // initially absent
      case _ => false // we can't change the count by more than 1.
    }

    sizeIsConsistent && added.contains(k)
  }

  property("updateFirst works") = forAll { (hmap: HMap[Key, Value]) =>
    val partial = new GenPartial[Key, Value] {
      def apply[T] = { case Key(id) if (id % 2 == 0) => Value(0) }
    }
    hmap.updateFirst(partial) match {
      case Some((updated, k)) => updated.get(k) == Some(Value(0))
      case None => true
    }
  }

  property("collect works") = forAll { (map: Map[Key[Int], Value[Int]]) =>
    val hm = map.foldLeft(HMap.empty[Key, Value])(_ + _)
    val partial = new GenPartial[HMap[Key, Value]#Pair, Value] {
      def apply[T] = { case (Key(k), Value(v)) if k > v => Value(k * v) }
    }
    val collected = hm.collect(partial).map { case Value(v) => v }.toSet
    val mapCollected = map.collect(partial.apply[Int]).map { case Value(v) => v }.toSet
    collected == mapCollected
  }

  property("collectValues works") = forAll { (map: Map[Key[Int], Value[Int]]) =>
    val hm = map.foldLeft(HMap.empty[Key, Value])(_ + _)
    val partial = new GenPartial[Value, Value] {
      def apply[T] = { case Value(v) if v < 0 => Value(v * v) }
    }
    val collected = hm.collectValues(partial).map { case Value(v) => v }.toSet
    val mapCollected = map.values.collect(partial.apply[Int]).map { case Value(v) => v }.toSet
    collected == mapCollected
  }
}
