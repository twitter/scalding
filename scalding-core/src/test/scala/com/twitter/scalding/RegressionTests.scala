package com.twitter.scalding

import org.scalatest.FunSuite

class RegressionTests extends FunSuite {
  test("hashJoins + merges that fail in cascading 3") {
    val p1 =
      TypedPipe.from(List(1, 2))
        .cross(TypedPipe.from(List(3, 4)))

    val p2 =
      TypedPipe.from(List(5, 6))
        .cross(TypedPipe.from(List(8, 9)))

    val p3 = (p1 ++ p2)
    val p4 = (TypedPipe.from(List((8, 1), (10, 2))) ++ p3)

    val expected = List((1, 3), (1, 4), (2, 3), (2, 4), (5, 8), (5, 9), (6, 8), (6, 9), (8, 1), (10, 2))
    val values = p4.toIterableExecution
      .waitFor(Config.empty, Local(true))
      .get
    assert(values.toList.sorted == expected)
  }
}
