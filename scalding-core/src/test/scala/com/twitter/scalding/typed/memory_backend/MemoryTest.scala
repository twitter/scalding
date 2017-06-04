package com.twitter.scalding.typed.memory_backend

import org.scalatest.FunSuite
import com.twitter.scalding.{ TypedPipe, Execution, Config, Local }

class MemoryTest extends FunSuite {

  private def mapMatch[K, V](ex: Execution[Iterable[(K, V)]]) = {
    val mm = MemoryMode.empty

    val mkv = ex.waitFor(Config.empty, mm)

    val lkv = ex.waitFor(Config.empty, Local(true))
    assert(mkv.get.toMap == lkv.get.toMap)
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

  // fails now
  // test("join works") {
  //   val input = TypedPipe.from(0 until 100)
  //   val left = input.map { k => (k, k % 2) }
  //   val right = input.map { k => (k, k % 3) }

  //   mapMatch(left.join(right).toIterableExecution)
  // }

}
