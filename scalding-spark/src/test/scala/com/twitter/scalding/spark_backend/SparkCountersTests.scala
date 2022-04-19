package com.twitter.scalding.spark_backend

import com.twitter.scalding.{StatKey, TextLine}
import com.twitter.scalding.typed.TypedPipe

class SparkCountersTests extends SparkBaseTest {

  /**
   * Note tallyAll takes (group, counter) while StatKey takes (counter, group) which may be confusing. We run
   * these tests several times as a sanity check against race condition issues.
   */
  val testRuns = 20

  for (t <- 1 to testRuns) test(s"pure counters basic case run #$t") {
    val cpipe1 = TypedPipe
      .from(0 until 100)
      .tallyAll("scalding", "test")
    val cresult1 = sparkRetrieveCounters(cpipe1)
    assert(cresult1.toMap.size == 1)
    assert(cresult1.get(StatKey("test", "scalding")).get == 100)
  }

  for (t <- 1 to testRuns) test(s"pure counters write execution case #$t") {
    val sinkPath = tmpPath("countersTest")
    val sinkExample = TextLine(sinkPath)
    removeDir(sinkPath)
    val cpipe2 = TypedPipe
      .from(0 until 10)
      .tallyAll("scalding", "test")
      .map(_.toString)
      .writeExecution(sinkExample)
      .flatMap(_ => TypedPipe.from(sinkExample).toIterableExecution)
    val cresult2 = sparkRetrieveCounters(cpipe2)
    assert(cresult2.toMap.size == 1)
    assert(cresult2.get(StatKey("test", "scalding")).get == 10)
  }

  for (t <- 1 to testRuns) test(s"pure counters many transforms #$t") {
    val cpipe3 = TypedPipe
      .from(0 until 100)
      .filter(x => x % 4 == 0)
      .tallyAll("something interesting", "divisible by 4")

    val cpipe4 =
      cpipe3
        .cross(
          TypedPipe
            .from(0 to 10)
            .tallyBy("inner")(x => (if (x % 3 == 0) "divisible by 3" else "not divisible by 3"))
        )
        .tallyBy("outer")(x => (if (x._2 % 3 == 0) "divisible by 3" else "not divisible by 3"))
    val cresult3 = sparkRetrieveCounters(cpipe4)
    assert(cresult3.toMap.size == 5)
    assert(cresult3.get(StatKey("divisible by 4", "something interesting")).get == 25)
    assert(cresult3.get(StatKey("divisible by 3", "inner")).get == 4)
    assert(cresult3.get(StatKey("not divisible by 3", "inner")).get == 7)
    assert(cresult3.get(StatKey("divisible by 3", "outer")).get == 25 * 4)
    assert(cresult3.get(StatKey("not divisible by 3", "outer")).get == 25 * 7)
  }
}
