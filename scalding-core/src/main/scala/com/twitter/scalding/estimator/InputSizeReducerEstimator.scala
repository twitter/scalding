package com.twitter.scalding.estimator

import cascading.flow.{ FlowStep, Flow }
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList }

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object InputSizeReducerEstimator {
  val bytesPerReducer = "scalding.reducer.estimator.bytes.per.reducer"

  def bytesPerReducer(conf: JobConf): Long =
    conf.getLong(bytesPerReducer, 1L << 30)
}

class InputSizeReducerEstimator extends ReducerEstimator {
  import InputSizeReducerEstimator.bytesPerReducer

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

  override def reducerEstimate(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Option[Int] = {

    val conf = flowStep.getConfig
    val srcs = flowStep.getSources.asScala

    flowStep.getAllAccumulatedSources.asScala.foreach{ t =>
      println("@> acc -> " + t.getIdentifier)
    }

    srcs.foreach {
      case f: Hfs =>
        println("@> " + f.getIdentifier)
        println("@> - size: " + size(f, conf))

      //f.getChildIdentifiers(conf).foreach { c =>
      //  println("@> - " + c)
      //}
      case t =>
        println("@> tap " + t.getIdentifier)
    }

    // only try to make this estimate if we can get the size of all of the inputs
    if (srcs.forall(_.isInstanceOf[Hfs])) {

      val totalBytes = srcs.map(_.asInstanceOf[Hfs]).map(size(_, conf)).sum

      val nReducers = math.max(1, math.ceil(
        totalBytes.toDouble / bytesPerReducer(conf)).toInt)

      LOG.info("totalBytes = " + totalBytes)
      LOG.info("reducerEstimate = " + nReducers)

      Some(nReducers)

    } else {
      LOG.info("Unable to estimate reducers; not all input sizes available.")
      None
    }
  }
}
