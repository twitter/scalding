package com.twitter.scalding.reducer_estimation

import scala.collection.JavaConverters._
import cascading.flow.FlowStep
import cascading.tap.{ Tap, MultiSourceTap }
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object InputSizeReducerEstimator {
  val BytesPerReducer = "scalding.reducer.estimator.bytes.per.reducer"
  val defaultBytesPerReducer = 1L << 33 // 8 GB

  /** Get the target bytes/reducer from the JobConf */
  def getBytesPerReducer(conf: JobConf): Long = conf.getLong(BytesPerReducer, defaultBytesPerReducer)
}

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 1 GB.
 */
class InputSizeReducerEstimator extends ReducerEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private def unrollTaps(taps: Seq[Tap[_, _, _]]): Seq[Tap[_, _, _]] =
    taps.flatMap {
      case multi: MultiSourceTap[_, _, _] =>
        unrollTaps(multi.getChildTaps.asScala.toSeq)
      case t => Seq(t)
    }

  private def unrolledSources(step: FlowStep[JobConf]): Seq[Tap[_, _, _]] =
    unrollTaps(step.getSources.asScala.toSeq)

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

  private def inputSizes(taps: Seq[Tap[_, _, _]], conf: JobConf): Option[Seq[(String, Long)]] = {
    if (taps.forall(_.isInstanceOf[Hfs])) {
      Some(taps.map(t => t.toString -> size(t.asInstanceOf[Hfs], conf)))
    } else {
      None
    }
  }

  protected def inputSizes(step: FlowStep[JobConf]): Option[Seq[(String, Long)]] =
    inputSizes(unrolledSources(step), step.getConfig)

  /**
   * Figure out the total size of the input to the current step and set the number
   * of reducers using the "bytesPerReducer" configuration parameter.
   */
  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    inputSizes(info.step) match {
      case Some(inputSizes) =>
        val bytesPerReducer =
          InputSizeReducerEstimator.getBytesPerReducer(info.step.getConfig)

        val totalBytes = inputSizes.map(_._2).sum
        val nReducers = (totalBytes.toDouble / bytesPerReducer).ceil.toInt max 1

        lazy val logStr = inputSizes.map {
          case (name, bytes) => "   - %s\t%d\n".format(name, bytes)
        }.mkString("")

        LOG.info("\nInputSizeReducerEstimator" +
          "\n - input size (bytes): " + totalBytes +
          "\n - reducer estimate:   " + nReducers +
          "\n - Breakdown:\n" +
          logStr)

        Some(nReducers)

      case None =>
        LOG.warn("InputSizeReducerEstimator unable to estimate reducers; " +
          "cannot compute size of:\n - " +
          unrolledSources(info.step).filterNot(_.isInstanceOf[Hfs]).mkString("\n - "))
        None
    }
}
