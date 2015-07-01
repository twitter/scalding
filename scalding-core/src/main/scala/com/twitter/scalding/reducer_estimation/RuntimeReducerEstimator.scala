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
trait RuntimeReducerEstimator extends HistoryReducerEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def estimateReducers(info: FlowStrategyInfo, history: Seq[FlowStepHistory]): Option[Int] = {
    val historicalTime = estimateJobTime(history.flatMap { h =>
      estimateTaskTime(h.tasks.collect {
        case t: Task if t.taskType == "REDUCE" && t.status == "SUCCEEDED" && t.finishTime > t.startTime => t.finishTime - t.startTime
      })
    })
    historicalTime.map { t =>
      (t.toDouble / RuntimeReducerEstimator.getRuntimePerReducer(info.step.getConfig)).ceil.toInt
    }
  }

  def estimateJobTime(times: Seq[Long]): Option[Long]
  def estimateTaskTime(times: Seq[Long]): Option[Long]
}

trait RuntimeMedianReducerEstimator extends RuntimeReducerEstimator {

  /** returns the median of the list (or None for an empty list) */
  def median(times: Seq[Long]): Option[Long] =
    times.sorted.lift(times.length / 2)

  def estimateJobTime(times: Seq[Long]) = median(times)
  def estimateTaskTime(times: Seq[Long]) = median(times).map(_ * times.length)
}

trait RuntimeMeanReducerEstimator extends RuntimeReducerEstimator {
  /** returns the mean of the list (or None for an empty list) */
  def mean(times: Seq[Long]): Option[Long] =
    if (times.isEmpty) None else Some(times.sum / times.length)

  def estimateJobTime(times: Seq[Long]) = mean(times)
  def estimateTaskTime(times: Seq[Long]) = Some(times.sum)
}
