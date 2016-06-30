package com.twitter.scalding

import cascading.flow.FlowException
import org.scalatest.{ Matchers, WordSpec }

import scala.util.Try

class StatsTestJob1(args: Args) extends Job(args) with CounterVerification {
  val nonZero = Stat("number of non-zero records", "stats")

  TypedPipe.from(TypedTsv[(String, Int)](args("input")))
    .map { kv =>
      if (kv._2 != 0) nonZero.inc()
      (kv._1.toLowerCase, kv._2)
    }
    .write(TypedTsv[(String, Int)](args("output")))

  override def verifyCounters(counters: Map[StatKey, Long]): Try[Unit] = Try {
    assert(counters(nonZero) > 0)
  }
}

class StatsTestJob2(args: Args) extends StatsTestJob1(args) {
  override def verifyCountersInTest: Boolean = false
}

class StatsTest extends WordSpec with Matchers {

  val goodInput = List(("a", 0), ("b", 1), ("c", 2))
  val badInput = List(("a", 0), ("b", 0), ("c", 0))

  def runJobTest[T: TupleSetter](f: Args => Job, input: List[T]): Unit = {
    JobTest(f)
      .arg("input", "input")
      .arg("output", "output")
      .source(TypedTsv[(String, Int)]("input"), input)
      .sink[(String, Int)](TypedTsv[(String, Int)]("output")){ outBuf => outBuf shouldBe input }
      .run
  }

  "StatsTestJob" should {
    "pass if verifyCounters() is true" in {
      runJobTest(new StatsTestJob1(_), goodInput)
    }
  }

  it should {
    "fail if verifyCounters() is false" in {
      an[FlowException] should be thrownBy runJobTest(new StatsTestJob1(_), badInput)
    }
  }

  it should {
    "skip verifyCounters() if job fails" in {
      (the[FlowException] thrownBy runJobTest(new StatsTestJob1(_), List((null, 0)))).getCause.getCause shouldBe a[NullPointerException]
    }
  }

  it should {
    "skip verifyCounters() if verifyCountersInTest is false" in {
      runJobTest(new StatsTestJob2(_), badInput)
    }
  }

}