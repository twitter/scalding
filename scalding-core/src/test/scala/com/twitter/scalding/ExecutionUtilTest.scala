package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

import scala.util.Try

class ExecutionUtilTest extends WordSpec with Matchers {
  import ExecutionUtil._

  implicit val tz = DateOps.UTC
  implicit val dp = DateParser.default
  implicit val dateRange = DateRange.parse("2015-01-01", "2015-01-10")

  def run[T](e: Execution[T]) = e.waitFor(Config.default, Local(true))

  def testJob(dr: DateRange) =
    TypedPipe
      .from[Int](Seq(1, 2, 3))
      .toIterableExecution
      .map(_.head)

  def testJobFailure(dr: DateRange) = throw new Exception("failed")

  "ExecutionUtil" should {
    "run multiple jobs" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = runDatesWithParallelism(Days(1))(testJob)
      assert(run(result).get == days.map(d => (d, 1)))
    }

    "run multiple jobs with executions" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = runDateRangeWithParallelism(Days(1))(testJob)
      assert(run(result).get == days.map(d => 1))
    }

    "run multiple jobs with executions and sum results" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = runDateRangeWithParallelismSum(Days(1))(testJob)
      assert(run(result).get == days.map(d => 1).sum)
    }
  }
}
