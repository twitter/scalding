package com.twitter.scalding.reducer_estimation

import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

import scala.util.{ Failure, Success }

object RatioBasedEstimator {
  /**
   * RatioBasedEstimator optionally ignores history items whose input size is
   * drastically different than the current job. This parameter specifies the
   * lower bound on allowable input size ratio. Defaults to 0.10 (10%), which
   * sets the upper bound to 10x.
   */
  val inputRatioThresholdKey = "scalding.reducer.estimator.input.ratio.threshold"
  def getInputRatioThreshold(conf: JobConf) = conf.getFloat(inputRatioThresholdKey, 0.10f)
}

abstract class RatioBasedEstimator extends InputSizeReducerEstimator with HistoryService {

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
    if (threshold > 0 && (ratio < threshold || ratio > 1 / threshold)) {
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
    val conf = info.step.getConfig
    val maxHistory = EstimatorConfig.getMaxHistory(conf)
    val threshold = RatioBasedEstimator.getInputRatioThreshold(conf)

    fetchHistory(info.step, maxHistory) match {
      case Success(history) =>
        val ratios = for {
          h <- history
          inputBytes <- super.inputSizes(info.step).map(_.map(_._2).sum)
          if acceptableInputRatio(inputBytes, h.mapperBytes, threshold)
        } yield h.reducerBytes / h.mapperBytes.toDouble

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
      case Failure(e) =>
        LOG.warn("Unable to fetch history. Disabling RatioBasedEstimator.")
        None
    }
  }

}
