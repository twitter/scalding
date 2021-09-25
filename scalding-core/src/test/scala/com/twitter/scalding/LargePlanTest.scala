package com.twitter.scalding

import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * on branch 0.17.x:
 * - size=2 took 0.5 seconds
 * - size=4 took 0.2 seconds
 * - size=8 took 0.3 seconds
 * - size=16 took 0.4 seconds
 * - size=32 took 0.7 seconds
 * - size=64 took 18.9 seconds
 * - size=128 timed out (after 60 seconds)
 *
 * on branch cascading3:
 * - size=2 took 0.6 seconds
 * - size=4 took 0.3 seconds
 * - size=8 took 0.3 seconds
 * - size=16 took 0.4 seconds
 * - size=32 took 0.5 seconds
 * - size=64 took 1.2 seconds
 * - size=128 took 2.7 seconds
 */

class LargePlanTest extends FunSuite {

  val ns = List((1, 100), (2, 200))

  // build a small pipe (only 2 keys) composed of a potentially large
  // number of joins.
  def build(size: Int): TypedPipe[(Int, Int)] = {
    val pipe = TypedPipe.from(ns)
    if (size <= 0) pipe
    else pipe.join(build(size - 1)).mapValues { case (x, y) => x + y }
  }

  // each test might run for up to this long
  val Timeout = 60.seconds // one minute

  // run a test at a particular size
  def run(size: Int): Unit = {
    val t0 = System.currentTimeMillis()
    val pipe = build(size)
    val exec = pipe.toIterableExecution
    val fut = exec.run(Config.empty, Local(true))
    val values = Await.result(fut, Timeout)
    val secs = "%.1f" format ((System.currentTimeMillis() - t0) / 1000.0)
    assert(true)
    println(s"size=$size took $secs seconds")
  }

  test("size=2") { run(2) }
  test("size=4") { run(4) }
  test("size=8") { run(8) }
  test("size=16") { run(16) }
  test("size=32") { run(32) }
  test("size=64") { run(64) }
  // test("size=128") { run(128) }
}

