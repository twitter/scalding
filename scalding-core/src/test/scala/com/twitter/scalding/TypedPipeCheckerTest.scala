package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

class TypedPipeCheckerTest extends WordSpec with Matchers {
  import TypedPipeChecker._

  "TypedPipeChecker" should {
    "run asserts on pipe" in {
      checkOutput(TypedPipe.from(List(1, 2, 3, 4))){ rows =>
        assert(rows.size == 4)
        assert(rows == List(1, 2, 3, 4))
      }
    }
  }

  it should {
    "give back a list" in {
      val list = checkOutputInline(TypedPipe.from(List(1, 2, 3, 4)))
      assert(list == List(1, 2, 3, 4))
    }
  }
}
