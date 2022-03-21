package com.twitter.scalding.dagon

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Cogen, Properties}

abstract class CacheTests[K: Cogen: Arbitrary, V: Arbitrary](name: String) extends Properties(name) {

  def buildMap(c: Cache[K, V], ks: Iterable[K], f: K => V): Map[K, V] =
    ks.iterator.foldLeft(Map.empty[K, V]) {
      (m, k) => m.updated(k, c.getOrElseUpdate(k, f(k)))
    }

  property("getOrElseUpdate") =
    forAll { (f: K => V, k: K, v1: V, v2: V) =>
      val c = Cache.empty[K, V]
      var count = 0
      val x = c.getOrElseUpdate(k, { count += 1; v1 })
      val y = c.getOrElseUpdate(k, { count += 1; v2 })
      x == v1 && y == v1 && count == 1
    }

  property("toMap") =
    forAll { (f: K => V, ks: Set[K]) =>
      val c = Cache.empty[K, V]
      val m = buildMap(c, ks, f)
      c.toMap == m
    }

  property("duplicate") =
    forAll { (f: K => V, ks: Set[K]) =>
      val c = Cache.empty[K, V]
      val d = c.duplicate
      buildMap(c, ks, f)
      d.toMap.isEmpty
    }

  property("reset works") =
    forAll { (f: K => V, ks: Set[K]) =>
      val c = Cache.empty[K, V]
      buildMap(c, ks, f)
      val d = c.duplicate
      c.reset()
      c.toMap.isEmpty && d.toMap.size == ks.size
    }
}

object CacheTestsSL extends CacheTests[String, Long]("CacheTests[String, Long]")
