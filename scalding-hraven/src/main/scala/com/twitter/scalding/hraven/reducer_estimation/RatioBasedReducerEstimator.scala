package com.twitter.scalding.hraven.reducer_estimation

import com.twitter.hraven.JobDetails
import com.twitter.scalding.reducer_estimation.{ FlowStrategyInfo, InputSizeReducerEstimator }
import org.slf4j.LoggerFactory

class RatioBasedReducerEstimator extends InputSizeReducerEstimator with HRavenHistory {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  /** use just InputSizeReducerEstimator if this estimator fails in any way */
  override lazy val fallbackEstimator = Some(new InputSizeReducerEstimator)

  protected def totalInputSize(pastStep: JobDetails): Option[Long] = {
    val pastInputBytes = pastStep.getHdfsBytesRead
    if (pastInputBytes <= 0) {
      LOG.warn("Invalid value in JobDetails: HdfsBytesRead = " + pastInputBytes)
      None
    } else {
      Some(pastInputBytes)
    }
  }

  protected def reducerSize(pastStep: JobDetails): Option[Long] = {
    val reducerBytes = pastStep.getReduceFileBytesRead
    if (reducerBytes <= 0) {
      LOG.warn("Invalid value in JobDetails: ReduceFileBytesRead = " + reducerBytes)
      None
    } else {
      Some(reducerBytes)
    }
  }

  /**
   * Determines if this input and the previous input are close enough.
   * If they're drastically different, we have no business trying to
   * make an estimate based on the past job.
   *
   * @param threshold  Specify lower bound on ratio (e.g. 0.10 for 10%)
   */
  private def acceptableInputRatio(current: Long, past: Long, threshold: Double): Option[Double] = {
    val ratio = current / past.toDouble
    if (ratio < threshold || ratio > 1 / threshold) {
      LOG.warn("Input sizes differ too much to make an informed decision:" +
        "\n  past bytes = " + past +
        "\n  this bytes = " + current)
      None
    } else {
      Some(ratio)
    }
  }

  /**
   * Estimate reducers the same way as InputSizeReducerEstimator
   * (using bytes.per.reducer), but scale the estimate based on
   * how much data actually went to the reducers in the last run.
   */
  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    for {
      pastStep <- fetchPastJobDetails(info.step)
      pastInputBytes <- totalInputSize(pastStep)
      inputBytes <- totalInputSize(info.step)
      inputRatio <- acceptableInputRatio(inputBytes, pastInputBytes, threshold = 0.1)
      baseEstimate <- super.estimateReducers(info)
      pastReducerBytes <- reducerSize(pastStep)
    } yield {
      val reducerRatio = pastReducerBytes / pastInputBytes.toDouble
      // scale reducer estimate based on the historical input ratio
      val e = (baseEstimate * reducerRatio).ceil.toInt max 1

      LOG.info("\nRatioBasedReducerEstimator"
        + "\n - past reducer ratio: " + reducerRatio
        + "\n - reducer estimate:   " + e)

      e
    }

}