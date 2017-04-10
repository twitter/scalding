package com.twitter.scalding.reducer_estimation

import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object InputSizeReducerEstimator {
  private[this] val LOG = LoggerFactory.getLogger(this.getClass)

  val BytesPerReducer = "scalding.reducer.estimator.bytes.per.reducer"
  val defaultBytesPerReducer = 1L << 32 // 4 GB

  /**
   * Get the target bytes/reducer from the JobConf.
   * Supported formats are <code>long</code> or human readable format.
   * For human readable format you can use the following suffix (case insensitive):
   * k(kilo), m(mega), g(giga), t(tera), p(peta), e(exa).
   *
   * Examples: 1024, 128m, 1g.
   */
  def getBytesPerReducer(conf: JobConf): Long =
    conf.getLongBytes(BytesPerReducer, defaultBytesPerReducer)

  /**
   * Same as estimateReducers, except doesn't round or ceil the result.
   * This is useful for composing with other estimation strategies that
   * don't want to lose the fractional number of reducers. Especially
   * helpful for when less than 1 reducer is needed, but this fraction
   * will be multiplied by a scaling factor later.
   */
  def estimateReducersWithoutRounding(info: FlowStrategyInfo): Option[Double] = {
    Common.inputSizes(info.step) match {
      case Nil =>
        LOG.warn("InputSizeReducerEstimator unable to estimate reducers; " +
          "cannot compute size of (is it a non hfs tap?):\n - " +
          Common.unrollTaps(info.step).filterNot(_.isInstanceOf[Hfs]).mkString("\n - "))
        None
      case inputSizes =>
        val bytesPerReducer =
          InputSizeReducerEstimator.getBytesPerReducer(info.step.getConfig)

        val totalBytes = inputSizes.map(_._2).sum
        val nReducers = totalBytes.toDouble / bytesPerReducer.toDouble

        lazy val logStr = inputSizes.map {
          case (name, bytes) => s"   - $name\t$bytes"
        }.mkString("\n")

        LOG.info("\nInputSizeReducerEstimator" +
          "\n - input size (bytes): " + totalBytes +
          "\n - reducer estimate:   " + nReducers +
          "\n - Breakdown:\n" +
          logStr)

        Some(nReducers)
    }
  }

}

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 4 GB.
 */
class InputSizeReducerEstimator extends ReducerEstimator {
  import InputSizeReducerEstimator._

  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    estimateReducersWithoutRounding(info).map { _.ceil.toInt.max(1) }
}