package com.twitter.scalding

import scala.collection.mutable

import com.twitter.algebird.Monoid
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success }

object ExecutionUtil {
  private[ExecutionUtil] class AsyncSemaphore(initialPermits: Int = 0) {
    private[this] val waiters = new mutable.Queue[() => Unit]
    private[this] var availablePermits = initialPermits

    private[ExecutionUtil] class SemaphorePermit {
      def release() =
        AsyncSemaphore.this.synchronized {
          availablePermits += 1
          if (availablePermits > 0 && waiters.nonEmpty) {
            availablePermits -= 1
            waiters.dequeue()()
          }
        }
    }

    def acquire(): Future[SemaphorePermit] = {
      val promise = Promise[SemaphorePermit]()

      def setAcquired(): Unit =
        promise.success(new SemaphorePermit)

      synchronized {
        if (availablePermits > 0) {
          availablePermits -= 1
          setAcquired()
        } else {
          waiters.enqueue(setAcquired)
        }
      }

      promise.future
    }
  }

  /**
   * Run a sequence of executions but only permitting parallelism amount to run at the
   * same time.
   *
   * @param executions List of executions to run
   * @param parallelism Number to run in parallel
   * @return Execution Seq
   */
  def withParallelism[T](executions: Seq[Execution[T]], parallelism: Int): Execution[Seq[T]] = {
    require(parallelism > 0, s"Parallelism must be > 0: $parallelism")
    val sem = new AsyncSemaphore(parallelism)

    def waitRun(e: Execution[T]): Execution[T] = {
      Execution.fromFuture(_ => sem.acquire())
        .flatMap(p => e.liftToTry.map((_, p)))
        .onComplete {
          case Success((_, p)) => p.release()
          case Failure(_) => ()
        }
        .map(_._1.get)
    }
    Execution.sequence(executions.map(waitRun))
  }

  /**
   * Generate a list of executions from a date range
   *
   * @param duration Duration to split daterange
   * @param parallelism How many jobs to run in parallel
   * @param fn Function to run a execution given a date range
   * @return Sequence of Executions per Day
   */
  def executionsFromDates[T](duration: Duration, parallelism: Int = 1)(fn: DateRange => Execution[T])(implicit dr: DateRange): Seq[Execution[T]] =
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
    withParallelism(dates.map(fn), parallelism).map(e => dates.zip(e))
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
  def runDateRangeWithParallelismSum[T](duration: Duration, parallelism: Int = 1)(fn: DateRange => Execution[T])(implicit dr: DateRange, mon: Monoid[T]): Execution[T] =
    runDateRangeWithParallelism(duration, parallelism)(fn)(dr).map(Monoid.sum[T])
}
