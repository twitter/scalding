package com.twitter.scalding.dagon

import org.scalatest.FunSuite

class MemoizeTests extends FunSuite {
  test("fibonacci is linear in time") {

    var calls = 0

    val fib =
      Memoize.function[Int, Long] { (i, f) =>
        calls += 1

        i match {
          case 0 => 0
          case 1 => 1
          case i => f(i - 1) + f(i - 2)
        }
      }

    def fib2(n: Int, x: Long, y: Long): Long =
      if (n == 0) x
      else fib2(n - 1, y, x + y)

    assert(fib(100) == fib2(100, 0L, 1L))
    assert(calls == 101)
  }

  test("functionK repeated calls only evaluate once") {

    var calls = 0
    val fn =
      Memoize.functionK[BoolT, BoolT](new Memoize.RecursiveK[BoolT, BoolT] {
        def toFunction[T] = {
          case (b, rec) =>
            calls += 1

            !b
        }
      })

    assert(fn(true) == false)
    assert(calls == 1)
    assert(fn(true) == false)
    assert(calls == 1)

    assert(fn(false) == true)
    assert(calls == 2)
    assert(fn(false) == true)
    assert(calls == 2)

  }
}
