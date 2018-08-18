package com.twitter.scalding.spark_backend

import org.scalacheck.Arbitrary
import org.scalatest.prop.PropertyChecks
import org.scalatest.PropSpec

import Arbitrary.arbitrary

class IteratorsTest extends PropSpec with PropertyChecks {

  class Elem[K: Ordering, V](input: List[(K, V)]) {
    val elems = input.sortBy(_._1)

    def it: Iterator[(K, V)] = elems.iterator

    def laws() = {
      law0()
      law1()
      law2()
      law3()
    }

    def law0() = {
      val got = Iterators.groupSequential(it).map(_._2.length).sum
      assert(got == elems.size)
    }

    def law1() = {
      val got = for {
        (k, vs) <- Iterators.groupSequential(it)
        v <- vs
      } yield (k, v)

      assert(got.toList == elems)
    }

    def law2() = {
      val got = Iterators
        .groupSequential(it)
        .map { case (k, vs) => (k, vs.toList) }

      val expected =
        elems
          .groupBy(_._1)
          .mapValues(_.map(_._2))
          .toList
          .sortBy(_._1)

      assert(got.toList == expected)
    }

    def law3() = {
      val got = Iterators.groupSequential(it).map(_._1).toList
      val expected = elems.map(_._1).distinct.sorted
      assert(got == expected)
    }
  }

  object Elem {
    implicit def arbitraryElem[K: Arbitrary: Ordering, V: Arbitrary]: Arbitrary[Elem[K, V]] =
      Arbitrary(arbitrary[List[(K, V)]].map(new Elem(_)))
  }

  property("laws hold for Elem[Int, Int]") {
    forAll { (e: Elem[Int, Int]) =>
      e.laws()
    }
  }

  property("laws hold for Elem[Boolean, Int]") {
    forAll { (e: Elem[Boolean, Int]) =>
      e.laws()
    }
  }

  property("laws hold for Elem[String, Int]") {
    forAll { (e: Elem[String, Int]) =>
      e.laws()
    }
  }

  property("laws hold for Elem[Unit, Int]") {
    forAll { (e: Elem[Unit, Int]) =>
      e.laws()
    }
  }
}
