package com.twitter.scalding.typed

import org.scalatest.FunSuite

import com.twitter.scalding.typed.functions.EqTypes

class ResolverTest extends FunSuite {

  class Key[A]
  class Value[A]

  val k1 = new Key[Int]
  val k2 = new Key[Int]
  val k3 = new Key[Int]
  val v1 = new Value[Int]
  val v2 = new Value[Int]
  val v3 = new Value[Int]

  // if they are eq, they have the same type
  def keq[A, B](ka: Key[A], kb: Key[B]): Option[EqTypes[A, B]] =
    if (ka == null || kb == null) None
    else if (ka eq kb) Some(EqTypes.reflexive[A].asInstanceOf[EqTypes[A, B]])
    else None

  val custom = new Resolver[Key, Value] {
    def apply[A](k: Key[A]) =
      keq(k1, k).map { eqtypes =>
        eqtypes.subst[Value](v3)
      }
  }

  import Resolver.pair

  test("orElse order is correct") {

    assert((pair(k1, v1) orElse pair(k1, v2))(k1) == Some(v1))
    assert((pair(k1, v2) orElse pair(k1, v1))(k1) == Some(v2))
    assert((pair(k2, v1) orElse pair(k1, v2))(k1) == Some(v2))
    assert((pair(k2, v2) orElse pair(k1, v1))(k1) == Some(v1))

    assert(((pair(k1, v1) orElse pair(k1, v2)) orElse pair(k1, v3))(k1) == Some(v1))
    assert(((pair(k1, v2) orElse pair(k1, v1)) orElse pair(k1, v3))(k1) == Some(v2))
    assert(((pair(k1, v1) orElse pair(k1, v2)) orElse pair(k2, v3))(k2) == Some(v3))

    assert(custom(k1) == Some(v3))
    assert(custom(k2) == None)

    assert((custom orElse pair(k1, v2))(k1) == Some(v3))
    assert((custom orElse pair(k2, v2))(k2) == Some(v2))
    assert((pair(k1, v2) orElse custom)(k1) == Some(v2))
    assert((pair(k2, v2) orElse custom)(k1) == Some(v3))
    assert((pair(k2, v2) orElse custom)(k2) == Some(v2))
  }

  test("test remapping with andThen") {
    val remap = Resolver.pair(k1, k2) orElse Resolver.pair(k2, k3) orElse Resolver.pair(k3, k1)

    assert((remap andThen (custom orElse pair(k1, v2)))(k1) == None)
    assert((remap andThen (custom orElse pair(k2, v2)))(k2) == None)
    assert((remap andThen (pair(k1, v2) orElse custom))(k3) == Some(v2))
    assert((remap andThen (pair(k2, v2) orElse custom))(k3) == Some(v3))
    assert((remap andThen (pair(k2, v2) orElse custom))(k1) == Some(v2))

  }
}
