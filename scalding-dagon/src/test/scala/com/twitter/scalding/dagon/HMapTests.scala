/*
 Copyright 2014 Twitter, Inc.
 Copyright 2017 Stripe, Inc.

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

package com.twitter.scalding.dagon

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Cogen, Gen, Properties}
import Arbitrary.arbitrary

/**
 * This tests the HMap. We use the type system to
 * prove the types are correct and don't (yet?) engage
 * in the problem of higher kinded Arbitraries.
 */
object HMapTests extends Properties("HMap") {

  case class Key[T](key: Int)

  object Key {
    implicit def arbitraryKey[A]: Arbitrary[Key[A]] =
      Arbitrary(arbitrary[Int].map(n => Key(n & 0xff)))
    implicit def cogenKey[A]: Cogen[Key[A]] =
      Cogen[Int].contramap(_.key)
  }

  case class Value[T](value: Int)

  object Value {
    implicit def arbitraryValue[A]: Arbitrary[Value[A]] =
      Arbitrary(arbitrary[Int].map(n => Value(n & 0xff)))
    implicit def cogenValue[A]: Cogen[Value[A]] =
      Cogen[Int].contramap(_.value)
  }

  type H = HMap[Key, Value]
  type K = Key[Int]
  type V = Value[Int]

  def fromPairs(kvs: Iterable[(K, V)]): H =
    kvs.foldLeft(HMap.empty[Key, Value])(_ + _)

  implicit val arbitraryHmap: Arbitrary[H] =
    Arbitrary(Gen.listOf(for {
      k <- arbitrary[K]
      v <- arbitrary[V]
    } yield (k, v)).map(fromPairs))

  type FK = FunctionK[H#Pair, Lambda[x => Option[Value[x]]]]
  type FKValues = FunctionK[Value, Value]

  implicit val arbitraryFunctionK: Arbitrary[FK] =
    Arbitrary(arbitrary[(Int, Int) => Option[Int]].map { f =>
      new FK {
        def toFunction[T] = { case (Key(m), Value(n)) => f(m, n).map(Value(_)) }
      }
    })

  implicit val arbitraryFunctionKValues: Arbitrary[FKValues] =
    Arbitrary(arbitrary[Int => Int].map { f =>
      new FKValues {
        override def toFunction[T] = v => Value(f(v.value))
      }
    })

  property("equals works") = forAll { (m0: Map[K, V], m1: Map[K, V]) =>
    (fromPairs(m0) == fromPairs(m1)) == (m0 == m1)
  }

  property("hashCode/equals consistency") = forAll { (h0: H, h1: H) =>
    if (h0 == h1) h0.hashCode == h1.hashCode else true
  }

  property("contains/get consistency") = forAll { (h: H, k: K) =>
    h.get(k).isDefined == h.contains(k)
  }

  property("+/updated consistency") = forAll { (h: H, k: K, v: V) =>
    h.updated(k, v) == h + (k -> v)
  }

  property("adding a pair works") = forAll { (h0: H, k: K, v: V) =>
    val h1 = h0.updated(k, v)
    val expectedSize = if (h0.contains(k)) h0.size else h0.size + 1
    (h1.get(k) == Some(v)) && (h1.size == expectedSize)
  }

  property("apply works") = forAll { (h: H, k: K) =>
    scala.util.Try(h(k)).toOption == h.get(k)
  }

  property("size works") = forAll { (m: Map[K, V]) =>
    fromPairs(m).size == m.size
  }

  property("removing a key works") = forAll { (h0: H, k: K) =>
    val h1 = h0 - k
    val expectedSize = if (h0.contains(k)) h0.size - 1 else h0.size
    (h1.get(k) == None) && (h1.size == expectedSize)
  }

  property("keysOf works") = forAll { (h0: H, k: K, v: V) =>
    val h1 = h0.updated(k, v)
    val newKeys = h1.keysOf(v) -- h0.keysOf(v)

    val sizeIsConsistent = newKeys.size match {
      case 0 => h0.contains(k) // k was already set to v
      case 1 => h0.get(k).forall(_ != v) // k was not set to v
      case _ => false // this should not happen
    }

    h1.contains(k) && sizeIsConsistent
  }

  property("optionMap works") = forAll { (m: Map[K, V], f: FK) =>
    val h = fromPairs(m)
    val got = h.optionMap(f).map { case Value(v) => v }.toSet
    val expected = m.flatMap(f(_)).map { case Value(v) => v }.toSet
    got == expected
  }

  property("keySet works") = forAll { (m: Map[K, V]) =>
    m.keySet == fromPairs(m).keySet
  }

  property("filterKeys works") = forAll { (h: H, p0: K => Boolean) =>
    val p = p0.asInstanceOf[Key[_] => Boolean]
    val a = h.filterKeys(p)
    h.keySet.forall { k => p(k) == a.contains(k) }
  }

  property("forallKeys works") = forAll { (h: H, p0: K => Boolean) =>
    val p = p0.asInstanceOf[Key[_] => Boolean]
    h.forallKeys(p) == h.keySet.forall(p)
  }

  property("HMap.from works") = forAll { (m: Map[K, V]) =>
    HMap.from[Key, Value](m.asInstanceOf[Map[Key[_], Value[_]]]) == fromPairs(m)
  }

  property("heterogenous equality is false") =
    forAll { (h: H) =>
      h != null && h != 33
    }

  property("++ works") = forAll { (m1: Map[K, V], m2: Map[K, V]) =>
    fromPairs(m1) ++ fromPairs(m2) == fromPairs(m1 ++ m2)
  }

  property("mapValues works") =
    forAll { (m: Map[K, V], fk: FKValues) =>
      val h = fromPairs(m)
      val got = fromPairs(m).mapValues(fk)
      got.forallKeys({ k => got.get(k) == h.get(k).map(fk(_)) })
    }
}
