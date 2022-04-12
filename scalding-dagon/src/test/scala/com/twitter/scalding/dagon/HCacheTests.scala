package com.twitter.scalding.dagon

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Cogen, Properties}

abstract class HCacheTests[K[_], V[_]](name: String)(implicit
    ka: Arbitrary[K[Int]],
    kc: Cogen[K[Int]],
    va: Arbitrary[V[Int]]
) extends Properties(name) {

  def buildHMap(c: HCache[K, V], ks: Iterable[K[Int]], f: K[Int] => V[Int]): HMap[K, V] =
    ks.iterator.foldLeft(HMap.empty[K, V]) { (m, k) =>
      m.updated(k, c.getOrElseUpdate(k, f(k)))
    }

  property("getOrElseUpdate") = forAll { (f: K[Int] => V[Int], k: K[Int], v1: V[Int], v2: V[Int]) =>
    val c = HCache.empty[K, V]
    var count = 0
    val x = c.getOrElseUpdate(k, { count += 1; v1 })
    val y = c.getOrElseUpdate(k, { count += 1; v2 })
    x == v1 && y == v1 && count == 1
  }

  property("toHMap") = forAll { (f: K[Int] => V[Int], ks: Set[K[Int]]) =>
    val c = HCache.empty[K, V]
    val m = buildHMap(c, ks, f)
    c.toHMap == m
  }

  property("duplicate") = forAll { (f: K[Int] => V[Int], ks: Set[K[Int]]) =>
    val c = HCache.empty[K, V]
    val d = c.duplicate
    buildHMap(c, ks, f)
    d.toHMap.isEmpty
  }

  property("reset works") = forAll { (f: K[Int] => V[Int], ks: Set[K[Int]]) =>
    val c = HCache.empty[K, V]
    buildHMap(c, ks, f)
    val d = c.duplicate
    c.reset()
    c.toHMap.isEmpty && d.toHMap.size == ks.size
  }
}

object HCacheTestsLL extends HCacheTests[List, List]("HCacheTests[List, List]")
