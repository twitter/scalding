package com.twitter.scalding.reducer_estimation

import scala.collection.JavaConverters._
import cascading.flow.FlowStep
import cascading.tap.{ Tap, MultiSourceTap }
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object InputSizeReducerEstimator {
  val BytesPerReducer = "scalding.reducer.estimator.bytes.per.reducer"
  val oneGigaByte = 1L << 30

  /** Get the target bytes/reducer from the JobConf */
  def getBytesPerReducer(conf: JobConf): Long = conf.getLong(BytesPerReducer, oneGigaByte)
}

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 1 GB.
 */
class InputSizeReducerEstimator extends ReducerEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private def sources(step: FlowStep[JobConf]) = step.getSources.asScala.toIterator

  /**
   * Get the total size of the file(s) specified by the Hfs, which may contain a glob
   * pattern in its path, so we must be ready to handle that case.
   */
  protected def size(f: Hfs, conf: JobConf): Long = {
    val fs = f.getPath.getFileSystem(conf)
    fs.globStatus(f.getPath)
      .map{ s => fs.getContentSummary(s.getPath).getLength }
      .sum
  }

  protected def totalSize(taps: Iterator[Tap[_, _, _]], conf: JobConf): Option[Long] =
    taps.foldLeft(Option(0L)) {
      // recursive case
      case (Some(total), multi: MultiSourceTap[Tap[_, _, _], _, _]) =>
        totalSize(multi.getChildTaps.asScala, conf).map(total + _)
      // base case:
      case (Some(total), hfs: Hfs) =>
        Some(total + size(hfs, conf))
      // if any are not Hfs, then give up
      case _ => None
    }

  protected def totalInputSize(step: FlowStep[JobConf]): Option[Long] =
    totalSize(sources(step), step.getConfig)

  /**
   * Figure out the total size of the input to the current step and set the number
   * of reducers using the "bytesPerReducer" configuration parameter.
   */
  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    totalInputSize(info.step) match {
      case Some(totalBytes) =>
        val bytesPerReducer =
          InputSizeReducerEstimator.getBytesPerReducer(info.step.getConfig)

        val nReducers = (totalBytes.toDouble / bytesPerReducer).ceil.toInt max 1

        LOG.info("\nInputSizeReducerEstimator" +
          "\n - input size (bytes): " + totalBytes +
          "\n - reducer estimate:   " + nReducers)
        Some(nReducers)

      case None =>
        LOG.warn("InputSizeReducerEstimator unable to estimate reducers; " +
          "cannot compute size of:\n - " +
          sources(info.step).filterNot(_.isInstanceOf[Hfs]).mkString("\n - "))
        None
    }
}
