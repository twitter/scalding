package com.twitter.scalding.estimator

import java.util.{ List => JList }

import scala.collection.JavaConverters._
import cascading.flow.{ FlowStep, Flow }
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

object InputSizeReducerEstimator {
  val BytesPerReducer = "scalding.reducer.estimator.bytes.per.reducer"

  /** Get the target bytes/reducer from the JobConf (with default = 1 GB) */
  def getBytesPerReducer(conf: JobConf): Long = conf.getLong(BytesPerReducer, 1L << 30)
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

    // only try to make this estimate if we can get the size of all of the inputs
    if (srcs.forall(_.isInstanceOf[Hfs])) {

      val totalBytes = srcs.map(_.asInstanceOf[Hfs]).map(size(_, conf)).sum

      val bytesPerReducer = InputSizeReducerEstimator.getBytesPerReducer(conf)

      val nReducers = math.max(1, math.ceil(
        totalBytes.toDouble / bytesPerReducer).toInt)

      LOG.info("totalBytes = " + totalBytes)
      LOG.info("reducerEstimate = " + nReducers)

      Some(nReducers)

    } else {
      LOG.info("Unable to estimate reducers; not all input sizes available.")
      None
    }
  }
}
