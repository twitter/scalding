package com.twitter.scalding.parquet.scrooge

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.source.NullSink
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import org.scalatest.FunSuite

class PlanningTests extends FunSuite {
  // How many steps would this be in Hadoop on Cascading
  def steps[A](p: TypedPipe[A], opt: Boolean = true): Int = {
    val mode = Hdfs.default
    val fd = new FlowDef
    val pipe =
      if (opt) CascadingBackend.toPipe(p, NullSink.sinkFields)(fd, mode, NullSink.setter)
      else CascadingBackend.toPipeUnoptimized(p, NullSink.sinkFields)(fd, mode, NullSink.setter)
    NullSink.writeFrom(pipe)(fd, mode)
    val ec = ExecutionContext.newContext(Config.defaultFrom(mode))(fd, mode)
    val flow = ec.buildFlow.get.get
    flow.getFlowSteps.size
  }

  // test for https://github.com/twitter/scalding/issues/1837
  test("merging source plus mapped source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).map(_ => null.asInstanceOf[MockThriftStruct]))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("filtering works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")

    val pipe =
      TypedPipe.from(src1).filter(_ => true)

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("filtering and mapping works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")

    val pipe =
      TypedPipe.from(src1).filter(_ => true).map(_ => 1)

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("mapping and filtering works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")

    val pipe =
      TypedPipe.from(src1).map(_ => 1).filter(_ => true)

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus filter source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).filter(_ => true))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus forceToDisk.filter source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).forceToDisk.filter(_ => true))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 2)
  }

  test("merging source plus forceToDisk source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).forceToDisk)

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 2)
  }

  test("merging source plus onComplete.filter source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).onComplete(() => println("done")).filter(_ => true))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus onComplete source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = TypedPipe.from(src1) ++
      TypedPipe.from(src2).onComplete(() => println("done"))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus withDescription.filter source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).withDescription("foo").filter(_ => true))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus debug.filter source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).debug.filter(_ => true))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus filter and map source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2).filter(_ => true).map(_ => null.asInstanceOf[MockThriftStruct]))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

  test("merging source plus map and filter source works") {
    val src1 = new FixedPathParquetScrooge[MockThriftStruct]("src1")
    val src2 = new FixedPathParquetScrooge[MockThriftStruct]("src2")

    val pipe = (TypedPipe.from(src1) ++
      TypedPipe.from(src2)
      .map(_ => null.asInstanceOf[MockThriftStruct])
      .filter(_ => true))

    assert(steps(pipe) == 1)
    assert(steps(pipe, false) == 1)
  }

}
