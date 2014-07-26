package com.twitter.scalding.estimator

import java.util.{ List => JList }

import scala.collection.JavaConverters._
import cascading.flow.{ FlowStep, Flow }
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

  /**
   * Get the total size of the file(s) specified by the Hfs, which may contain a glob
   * pattern in its path, so we must be ready to handle that case.
   */
  private def size(f: Hfs, conf: JobConf): Long = {
    val fs = f.getPath.getFileSystem(conf)
    fs.globStatus(f.getPath)
      .map{ s => fs.getContentSummary(s.getPath).getLength }
      .sum
  }

  /**
   * Figure out the total size of the input to the current step and set the number
   * of reducers using the "bytesPerReducer" configuration parameter.
   */
  override def estimateReducers(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Option[Int] = {

    val conf = flowStep.getConfig
    val srcs = flowStep.getSources.asScala

    srcs.foldLeft(Option(0L)) {
      case (Some(total), hfs: Hfs) => Some(total + size(hfs, conf))
      // if any are not Hfs, then give up
      case _ => None
    } match {
      case Some(totalBytes) =>
        val bytesPerReducer = InputSizeReducerEstimator.getBytesPerReducer(conf)

        val nReducers = math.max(1, math.ceil(
          totalBytes.toDouble / bytesPerReducer).toInt)

        LOG.info("totalBytes = " + totalBytes)
        LOG.info("reducerEstimate = " + nReducers)
        Some(nReducers)

      case None =>
        LOG.warn("Unable to estimate reducers; cannot compute size of:")
        srcs.filterNot(_.isInstanceOf[Hfs]).foreach { s => LOG.warn(" - " + s) }
        None
    }
  }
}
