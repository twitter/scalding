package com.twitter.scalding.parquet.scrooge

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.source.NullSink
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import org.scalatest.FunSuite

class PlanningTests extends FunSuite {
  // How many steps would this be in Hadoop on Cascading
  def steps[A](p: TypedPipe[A]): Int = {
    val mode = Hdfs.default
    val fd = new FlowDef
    val pipe = CascadingBackend.toPipe(p, NullSink.sinkFields)(fd, mode, NullSink.setter)
    NullSink.writeFrom(pipe)(fd, mode)
    val ec = ExecutionContext.newContext(Config.defaultFrom(mode))(fd, mode)
    val flow = ec.buildFlow.get
    flow.getFlowSteps.size
  }

  // test for https://github.com/twitter/scalding/issues/1837
  test("merging source plus mapped source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).map(_ => null.asInstanceOf[MockThriftStruct]))

    assert(steps(pipe) == 1)
  }

}
