package com.twitter.scalding.estimation

import cascading.flow.FlowStep
import cascading.tap.hadoop.Hfs
import cascading.tap.{ CompositeTap, Tap }
import com.twitter.scalding.tap.GlobHfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

object Common {
  private[this] val LOG = LoggerFactory.getLogger(this.getClass)

  private def unrollTaps(taps: Seq[Tap[_, _, _]]): Seq[Tap[_, _, _]] =
    taps.flatMap {
      case multi: CompositeTap[_] =>
        unrollTaps(multi.getChildTaps.asScala.toSeq)
      case t => Seq(t)
    }

  def unrollTaps(step: FlowStep[JobConf]): Seq[Tap[_, _, _]] =
    unrollTaps(step.getSources.asScala.toSeq)

  def inputSizes(step: FlowStep[JobConf]): Seq[(String, Long)] = {
    val conf = step.getConfig
    unrollTaps(step).flatMap {
      case tap: GlobHfs => Some(tap.toString -> tap.getSize(conf))
      case tap: Hfs => Some(tap.toString -> GlobHfs.getSize(tap.getPath, conf))
      case tap =>
        LOG.warn("InputSizeReducerEstimator unable to calculate size: " + tap)
        None
    }
  }

  def totalInputSize(step: FlowStep[JobConf]): Long = inputSizes(step).map(_._2).sum
}