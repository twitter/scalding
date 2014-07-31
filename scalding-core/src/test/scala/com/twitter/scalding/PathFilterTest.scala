package com.twitter.scalding

import org.specs.Specification
import org.apache.hadoop.fs.{ Path => HadoopPath, PathFilter }

class PathFilterTest extends Specification {
  "RichPathFilter" should {
    import RichPathFilter.toRichPathFilter
    val p = new HadoopPath("/nowhere")

    "compose ands" in {
      AlwaysTrue.and(AlwaysTrue).accept(p) must be_==(true)
      AlwaysTrue.and(AlwaysFalse).accept(p) must be_==(false)
      AlwaysFalse.and(AlwaysTrue).accept(p) must be_==(false)
      AlwaysFalse.and(AlwaysFalse).accept(p) must be_==(false)

      AlwaysTrue.and(AlwaysTrue, AlwaysTrue).accept(p) must be_==(true)
      AlwaysTrue.and(AlwaysTrue, AlwaysFalse).accept(p) must be_==(false)
    }

    "compose ors" in {
      AlwaysTrue.or(AlwaysTrue).accept(p) must be_==(true)
      AlwaysTrue.or(AlwaysFalse).accept(p) must be_==(true)
      AlwaysFalse.or(AlwaysTrue).accept(p) must be_==(true)
      AlwaysFalse.or(AlwaysFalse).accept(p) must be_==(false)

      AlwaysFalse.or(AlwaysTrue, AlwaysTrue).accept(p) must be_==(true)
      AlwaysTrue.or(AlwaysFalse, AlwaysFalse).accept(p) must be_==(true)
    }

    "negate nots" in {
      AlwaysTrue.not.accept(p) must be_==(false)
      AlwaysFalse.not.accept(p) must be_==(true)
      AlwaysTrue.not.not.accept(p) must be_==(true)
    }

  }
}

object AlwaysTrue extends PathFilter {
  override def accept(p: HadoopPath): Boolean = true
}

object AlwaysFalse extends PathFilter {
  override def accept(p: HadoopPath): Boolean = false
}