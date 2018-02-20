package com.twitter.scalding.typed.memory_backend

import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import com.twitter.scalding.{ TypedPipe, Execution, Config, Local }
import com.twitter.scalding.typed.TypedPipeGen

class MemoryTest extends FunSuite with PropertyChecks {

  private def mapMatch[K, V](ex: Execution[Iterable[(K, V)]]) = {
    val mm = MemoryMode.empty

    val mkv = ex.waitFor(Config.empty, mm)

    val lkv = ex.waitFor(Config.empty, Local(true))
    assert(mkv.get.toMap == lkv.get.toMap)
  }

  private def timeit[A](msg: String, a: => A): A = {
    val start = System.nanoTime()
    val res = a
    val diff = System.nanoTime() - start
    val ms = diff / 1e6
    // uncomment this for some poor version of benchmarking,
    // but scalding in-memory mode seems about 3-100x faster
    //
    // println(s"$msg: $ms ms")
    res
  }

  private def sortMatch[A: Ordering](ex: Execution[Iterable[A]]) = {
    val mm = MemoryMode.empty

    val mkv = timeit("scalding", ex.waitFor(Config.empty, mm))

    val lkv = timeit("cascading", ex.waitFor(Config.empty, Local(true)))
    assert(mkv.get.toList.sorted == lkv.get.toList.sorted)
  }

  test("basic word count") {
    val x = TypedPipe.from(0 until 100)
      .groupBy(_ % 2)
      .sum
      .toIterableExecution

    mapMatch(x)
  }

  test("mapGroup works") {
    val x = TypedPipe.from(0 until 100)
      .groupBy(_ % 2)
      .mapGroup { (k, vs) => Iterator.single(vs.foldLeft(k)(_ + _)) }
      .toIterableExecution

    mapMatch(x)
  }

  test("hashJoin works") {
    val input = TypedPipe.from(0 until 100)
    val left = input.map { k => (k, k % 2) }
    val right = input.map { k => (k, k % 3) }

    mapMatch(left.hashJoin(right).toIterableExecution)
  }

  test("join works") {
    val input = TypedPipe.from(0 until 100)
    val left = input.map { k => (k, k % 2) }
    val right = input.map { k => (k, k % 3) }

    mapMatch(left.join(right).toIterableExecution)
  }

  test("scalding memory mode matches cascading local mode") {
    import TypedPipeGen.genWithIterableSources
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 50)
    forAll(genWithIterableSources) { pipe => sortMatch(pipe.toIterableExecution) }
  }

  test("writing gives the same result as toIterableExecution") {
    import TypedPipeGen.genWithIterableSources
    // we can afford to test a lot more in just memory mode because it is faster than cascading
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)
    forAll(genWithIterableSources) { pipe =>
      val sink = new MemorySink.LocalVar[Int]

      val ex1 = pipe.writeExecution(SinkT("my_sink"))
      val ex2 = pipe.toIterableExecution

      val mm = MemoryMode.empty.addSink(SinkT("my_sink"), sink)
      val res1 = ex1.waitFor(Config.empty, mm)
      val res2 = ex2.waitFor(Config.empty, MemoryMode.empty)

      assert(sink.reset().get.toList.sorted == res2.get.toList.sorted)

    }
  }

  test("using sources work") {
    val srctag = SourceT[Int]("some_source")

    val job = TypedPipe.from(srctag).map { i => (i % 31, i) }.sumByKey.toIterableExecution

    val jobRes = job.waitFor(Config.empty, MemoryMode.empty.addSourceIterable(srctag, (0 to 10000)))

    val expected = (0 to 10000).groupBy(_ % 31).mapValues(_.sum).toList.sorted
    assert(jobRes.get.toList.sorted == expected)
  }
}
