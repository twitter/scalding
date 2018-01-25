package com.twitter.scalding

import com.twitter.algebird.Semigroup

object ExecutionUtil {
  /**
   * Generate a list of executions from a date range
   *
   * @param duration Duration to split daterange
   * @param fn Function to run a execution given a date range
   * @return Sequence of Executions per Day
   */
  def executionsFromDates[T](duration: Duration)(fn: DateRange => Execution[T])(implicit dr: DateRange): Seq[Execution[T]] =
    dr.each(duration).map(fn).toSeq

  /**
   * Split a DateRange and allow for max parallel running of executions
   *
   * @param duration Duration to split daterange
   * @param parallelism How many jobs to run in parallel
   * @param fn Function to run a execution given a date range
   * @return Seq of Dates split by Duration with corresponding execution result
   */
  def runDatesWithParallelism[T](duration: Duration, parallelism: Int = 1)(fn: DateRange => Execution[T])(implicit dr: DateRange): Execution[Seq[(DateRange, T)]] = {

    val dates = dr.each(duration).toSeq
    Execution.withParallelism(dates.map(fn), parallelism).map(e => dates.zip(e))
  }

  /**
   * Split a DateRange and allow for max parallel running of executions
   *
   * @param duration Duration to split daterange
   * @param parallelism How many jobs to run in parallel
   * @param fn Function to run a execution given a date range
   * @return Execution of Sequences
   */
  def runDateRangeWithParallelism[T](duration: Duration, parallelism: Int = 1)(fn: DateRange => Execution[T])(implicit dr: DateRange): Execution[Seq[T]] =
    runDatesWithParallelism(duration, parallelism)(fn).map(_.map{ case (_, t) => t })

  /**
   * Same as runDateRangeWithParallelism, but sums the sequence
   * of values after running. This is useful when you want to do a
   * calculation in parallel over many durations and join the results
   * together.
   *
   * For example, a common use case is when T is
   * a TypedPipe[U] and you want to independently compute
   * the pipes on each day and union them into a
   * single TypedPipe at the end.
   *
   * Another possible use case would be if the executions were created by
   * summing intermediate monoids (e.g. T was a Map[String,HLL] since
   * algebird supports monoids for maps and hll) and you wanted to do a
   * final aggregation of the Monoids computed for each duration.
   */
  def runDateRangeWithParallelismSum[T](duration: Duration, parallelism: Int = 1)(fn: DateRange => Execution[T])(implicit dr: DateRange, semigroup: Semigroup[T]): Execution[T] = {
    require(dr.each(duration).nonEmpty, "Date Range can not be empty")

    runDateRangeWithParallelism(duration, parallelism)(fn)(dr)
      .map(_.reduceLeft[T]{ case (l, r) => Semigroup.plus(l, r) })
  }
}
