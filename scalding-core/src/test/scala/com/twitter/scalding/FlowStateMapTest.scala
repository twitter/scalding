package com.twitter.scalding

import org.scalatest.FunSuite

import cascading.flow.FlowDef
import com.twitter.scalding.source.{ TypedText, NullSink }
import com.twitter.scalding.typed.cascading_backend.CascadingBackend

class FlowStateMapTest extends FunSuite {
  test("make sure sure sourcemap isn't empty after planning") {
    implicit val fd = new FlowDef
    implicit val m = Local(false)
    val t = TypedPipe.from(TypedText.tsv[String]("")).write(NullSink)
    CascadingBackend.planTypedWrites(fd, m)
    val state = FlowStateMap(fd)
    assert(state.sourceMap.nonEmpty)
  }
}
