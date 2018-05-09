package com.twitter.scalding
package typed

import com.twitter.algebird.Monoid.{ plus, sum, zero }
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

class TypedPipeMonoidTest extends FunSuite with PropertyChecks {

  def run[A](t: TypedPipe[A]): List[A] =
    t.toIterableExecution.map(_.toList).waitFor(Config.empty, Local(true)).get

  def sortedEq[A: Ordering](a: List[A], b: List[A]): Boolean =
    a.sorted == b.sorted

  def eqvPipe[A: Ordering](a: TypedPipe[A], b: TypedPipe[A]): Boolean =
    sortedEq(run(a), run(b))

  test("typedPipeMonoid.zero should be equal to TypePipe.empty") {
    assert(zero[TypedPipe[Int]] == TypedPipe.empty)
  }

  test("monoid is associative") {
    forAll { (a: List[Int], b: List[Int], c: List[Int]) =>
      val left = plus(plus(TypedPipe.from(a), TypedPipe.from(b)), TypedPipe.from(c))
      val right = plus(TypedPipe.from(a), plus(TypedPipe.from(b), TypedPipe.from(c)))
      assert(eqvPipe(left, right))
    }
  }

  test("monoid is commutative") {
    forAll { (a: List[Int], b: List[Int]) =>
      val left = plus(TypedPipe.from(a), TypedPipe.from(b))
      val right = plus(TypedPipe.from(b), TypedPipe.from(a))
      assert(eqvPipe(left, right))
    }
  }

  test("monoid sum is equivalent to a union") {
    forAll { (as: List[List[Int]]) =>
      val pipes = as.map(TypedPipe.from(_))
      val bigPipe = TypedPipe.from(as.flatten)
      assert(eqvPipe(sum(pipes), bigPipe))
    }
  }

}
