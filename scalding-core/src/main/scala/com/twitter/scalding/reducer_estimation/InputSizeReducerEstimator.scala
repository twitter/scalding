package com.twitter.scalding.reducer_estimation

import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object InputSizeReducerEstimator {
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
}

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 4 GB.
 */
class InputSizeReducerEstimator extends ReducerEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * Figure out the total size of the input to the current step and set the number
   * of reducers using the "bytesPerReducer" configuration parameter.
   */
  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    Common.inputSizes(info.step) match {
      case Nil =>
        LOG.warn("InputSizeReducerEstimator unable to estimate reducers; " +
          "cannot compute size of:\n - " +
          Common.unrollTaps(info.step).filterNot(_.isInstanceOf[Hfs]).mkString("\n - "))
        None
      case inputSizes =>
        val bytesPerReducer =
          InputSizeReducerEstimator.getBytesPerReducer(info.step.getConfig)

        val totalBytes = inputSizes.map(_._2).sum
        val nReducers = (totalBytes.toDouble / bytesPerReducer).ceil.toInt max 1

        lazy val logStr = inputSizes.map {
          case (name, bytes) => s"   - ${name}\t${bytes}"
        }.mkString("\n")

        LOG.info("\nInputSizeReducerEstimator" +
          "\n - input size (bytes): " + totalBytes +
          "\n - reducer estimate:   " + nReducers +
          "\n - Breakdown:\n" +
          logStr)

        Some(nReducers)
    }
}
