package com.twitter.scalding.reducer_estimation

import scala.collection.JavaConverters._
import cascading.flow.FlowStep
import cascading.tap.{ Tap, CompositeTap }
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object RuntimeReducerEstimator {
  val RuntimePerReducer = "scalding.reducer.estimator.runtime.per.reducer"
  val defaultRuntimePerReducer = 10 * 60 * 1000 // 10 mins

  /** Get the target bytes/reducer from the JobConf */
  def getRuntimePerReducer(conf: JobConf): Long = conf.getLong(RuntimePerReducer, defaultRuntimePerReducer)

  def getReduceTimes(history: Seq[FlowStepHistory]): Seq[Seq[Long]] =
    history.map { h =>
      h.tasks
        .filter { t => t.taskType == "REDUCE" && t.status == "SUCCEEDED" && t.finishTime > t.startTime }
        .map { t => t.finishTime - t.startTime }
    }
}

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 1 GB.
 */
trait RuntimeEstimationScheme {

  /**
   *  Given a list of times that each reducer took in a certain FlowStep,
   *  aggregates these times into a single estimate of the time that
   *  a "typical" reducer took.
   *  Suggested implementation: mean or median.
   */
  def estimateTaskTime(times: Seq[Long]): Option[Long]

  /**
   *  Given a list of "typical" times observed in a series of jobs of
   *  the same FlowStep, aggregates these times into a single estimate of
   *  the time that a "typical" reducer took in a "typical" job.
   *  Suggested implementation: mean or median.
   */
  def estimateJobTime(times: Seq[Long]): Option[Long]
}

trait BasicRuntimeReducerEstimator extends HistoryReducerEstimator {
  import RuntimeReducerEstimator._

  def runtimeEstimationScheme: RuntimeEstimationScheme

  def estimateReducers(info: FlowStrategyInfo, history: Seq[FlowStepHistory]): Option[Int] = {
    val reduceTimes: Seq[Seq[Long]] = getReduceTimes(history)

    // total time taken in the step = time per reducer * number of reducers
    val jobTimes: Seq[Option[Long]] = reduceTimes.map { xs => runtimeEstimationScheme.estimateTaskTime(xs).map(_ * xs.length) }

    // time per step, averaged over all the steps
    val typicalJobTime: Option[Long] = runtimeEstimationScheme.estimateJobTime(jobTimes.flatten)

    val desiredRuntime = getRuntimePerReducer(info.step.getConfig)
    typicalJobTime.map { t => (t.toDouble / desiredRuntime).ceil.toInt }
  }
}

trait InputScaledRuntimeReducerEstimator extends HistoryReducerEstimator {
  import RuntimeReducerEstimator._

  def runtimeEstimationScheme: RuntimeEstimationScheme

  def estimateReducers(info: FlowStrategyInfo, history: Seq[FlowStepHistory]): Option[Int] = {
    val reduceTimes: Seq[Seq[Long]] = getReduceTimes(history)

    // total time taken in the step = time per reducer * number of reducers
    val jobTimes: Seq[Option[Long]] = reduceTimes.map { xs => runtimeEstimationScheme.estimateTaskTime(xs).map(_ * xs.length) }

    // time-to-byte ratio for a step = time per reducer * number of reducers / number of bytes
    val timeToByteRatios: Seq[Long] =
      jobTimes.zip { history.map(_.hdfsBytesRead) }
        .collect { case (Some(time), bytes) => time / bytes }

    // time-to-byte ratio, averaged over all the steps
    val typicalTimeToByteRatio: Option[Long] = runtimeEstimationScheme.estimateJobTime(timeToByteRatios)

    val desiredRuntime = getRuntimePerReducer(info.step.getConfig)
    val inputBytes = Common.totalInputSize(info.step)

    if (inputBytes == 0) {
      None
    } else {
      // numReducers = time-per-byte * numBytes / desiredRuntime
      typicalTimeToByteRatio.map { t => (t.toDouble * inputBytes / desiredRuntime).ceil.toInt }
    }
  }
}

object MedianEstimationScheme extends RuntimeEstimationScheme {
  import reducer_estimation.{ mean, median }

  def estimateJobTime(times: Seq[Long]) = mean(times)
  def estimateTaskTime(times: Seq[Long]) = median(times)
}

object MeanEstimationScheme extends RuntimeEstimationScheme {
  import reducer_estimation.{ mean, median }

  def estimateJobTime(times: Seq[Long]) = mean(times)
  def estimateTaskTime(times: Seq[Long]) = mean(times)
}
