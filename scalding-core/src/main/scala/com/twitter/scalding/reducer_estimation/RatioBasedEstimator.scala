package com.twitter.scalding.reducer_estimation

import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object RatioBasedEstimator {
  /** Maximum number of history items to use for reducer estimation. */
  val maxHistoryKey = "scalding.reducer.estimator.max.history"

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 1)
}

abstract class RatioBasedEstimator extends InputSizeReducerEstimator with HistoryService {
  import RatioBasedEstimator._
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * Determines if this input and the previous input are close enough.
   * If they're drastically different, we have no business trying to
   * make an estimate based on the past job.
   *
   * @param threshold  Specify lower bound on ratio (e.g. 0.10 for 10%)
   */
  private def acceptableInputRatio(current: Long, past: Long, threshold: Double): Boolean = {
    val ratio = current / past.toDouble
    if (ratio < threshold || ratio > 1 / threshold) {
      LOG.warn("Input sizes differ too much to use for estimation: " +
        "current: " + current + ", past: " + past)
      false
    } else true
  }

  /**
   * Compute the average ratio of mapper bytes to reducer bytes and use that to
   * scale the estimate produced by InputSizeReducerEstimator.
   */
  override def estimateReducers(info: FlowStrategyInfo): Option[Int] = {
    val ratios = for {
      history <- fetchHistory(info.step, getMaxHistory(info.step.getConfig))
      inputBytes <- totalInputSize(info.step)
      if acceptableInputRatio(inputBytes, history.mapperBytes, threshold = 0.1)
    } yield history.reducerBytes / history.mapperBytes.toDouble

    if (ratios.length == 0) {
      LOG.warn("No matching history found.")
      None
    } else {
      val reducerRatio = ratios.sum / ratios.length
      super.estimateReducers(info).map { baseEstimate =>
        // scale reducer estimate based on the historical input ratio
        val e = (baseEstimate * reducerRatio).ceil.toInt max 1

        LOG.info("\nRatioBasedEstimator"
          + "\n - past reducer ratio: " + reducerRatio
          + "\n - reducer estimate:   " + e)

        e
      }
    }
  }

}
