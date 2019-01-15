package com.twitter.scalding

import cascading.flow.FlowException
import com.twitter.algebird.Monoid
import org.scalatest.{Matchers, WordSpec}

class MutatedValueBetweenStepsTest extends WordSpec with Matchers {
  case class Mutable(var value: Int)

  implicit object MutableMonoid extends Monoid[Mutable] {
    override def zero: Mutable = Mutable(0)

    override def plus(
      x: Mutable,
      y: Mutable
    ): Mutable = {
      // Let's avoid allocations =)
      y.value = x.value + y.value
      y
    }
  }

  "Merged pipe with mutable values" should {
    "fail with mutations in map" in {
      val items = List(5, 7, 11, 13)
      val input = TypedPipe.from(items) ++ TypedPipe.from(items)
      val result = input.map(Mutable).groupAll.sum
      assertThrows[FlowException](TypedPipeChecker.inMemoryToList(result))
    }
  }
}
