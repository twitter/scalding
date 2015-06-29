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
}

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 1 GB.
 */
trait RuntimeReducerEstimator extends DetailedHistoryReducerEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def estimateReducers(info: FlowStrategyInfo, history: Seq[DetailedFlowStepHistory]): Option[Int] = {
    val groupedSteps = history.groupBy(_.submitTime)
    val totalTimeByGroup = groupedSteps.mapValues(_.map(_.reducerTimeMillis).sum)
    val expectedTime = estimateExpectedTime(totalTimeByGroup.values.toSeq)
    val desiredTime = RuntimeReducerEstimator.getRuntimePerReducer(info.step.getConfig)
    expectedTime.map { t => (t.toDouble / desiredTime).ceil.toInt }.filter(_ > 0)
  }

  def estimateExpectedTime(reducerTimes: Seq[Long]): Option[Long]
}

trait RuntimeMedianReducerEstimator extends RuntimeReducerEstimator {

  /** returns the median of the list (or None for an empty list) */
  def estimateExpectedTime(reducerTimes: Seq[Long]): Option[Long] =
    reducerTimes.sorted.lift(reducerTimes.length / 2)
}

trait RuntimeMeanReducerEstimator extends RuntimeReducerEstimator {

  /** returns the mean of the list (or None for an empty list) */
  def estimateExpectedTime(times: Seq[Long]): Option[Long] =
    if (times.isEmpty) None else Some(times.sum / times.length)
}
