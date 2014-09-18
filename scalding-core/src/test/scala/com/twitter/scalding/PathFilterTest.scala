package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }
import org.apache.hadoop.fs.{ Path => HadoopPath, PathFilter }

class PathFilterTest extends WordSpec with Matchers {
  "RichPathFilter" should {
    import RichPathFilter.toRichPathFilter
    val p = new HadoopPath("/nowhere")

    "compose ands" in {
      AlwaysTrue.and(AlwaysTrue).accept(p) shouldBe true
      AlwaysTrue.and(AlwaysFalse).accept(p) shouldBe false
      AlwaysFalse.and(AlwaysTrue).accept(p) shouldBe false
      AlwaysFalse.and(AlwaysFalse).accept(p) shouldBe false

      AlwaysTrue.and(AlwaysTrue, AlwaysTrue).accept(p) shouldBe true
      AlwaysTrue.and(AlwaysTrue, AlwaysFalse).accept(p) shouldBe false
    }

    "compose ors" in {
      AlwaysTrue.or(AlwaysTrue).accept(p) shouldBe true
      AlwaysTrue.or(AlwaysFalse).accept(p) shouldBe true
      AlwaysFalse.or(AlwaysTrue).accept(p) shouldBe true
      AlwaysFalse.or(AlwaysFalse).accept(p) shouldBe false

      AlwaysFalse.or(AlwaysTrue, AlwaysTrue).accept(p) shouldBe true
      AlwaysTrue.or(AlwaysFalse, AlwaysFalse).accept(p) shouldBe true
    }

    "negate nots" in {
      AlwaysTrue.not.accept(p) shouldBe false
      AlwaysFalse.not.accept(p) shouldBe true
      AlwaysTrue.not.not.accept(p) shouldBe true
    }

  }
}

object AlwaysTrue extends PathFilter {
  override def accept(p: HadoopPath): Boolean = true
}

object AlwaysFalse extends PathFilter {
  override def accept(p: HadoopPath): Boolean = false
}
