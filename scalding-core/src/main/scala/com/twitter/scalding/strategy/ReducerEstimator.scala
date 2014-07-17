package com.twitter.scalding.strategy

import cascading.flow.{ FlowStep, Flow, FlowStepStrategy }
import cascading.tap.`type`.FileType
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList }

import org.slf4j.{ LoggerFactory, Logger }

import scala.collection.JavaConverters._

object ReducerEstimator extends FlowStepStrategy[JobConf] {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private val BYTES_PER_REDUCER_PARAM = "scalding.target.bytes.per.reducer"
  private val DEFAULT_BYTES_PER_REDUCER = 1L << 30 // 1 GB

  override def apply(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Unit = {
    val conf = flow.getConfig
    val srcs = flowStep.getSources.asScala

    srcs.foreach {
      case f: Hfs => println("@> path: " + f.getPath + ", size: " + f.getSize(conf))
    }

    if (srcs.forall(_.isInstanceOf[FileType[_]])) {
      val totalBytes = srcs.map{ case f: FileType[JobConf] => f.getSize(conf) }.reduce(_ + _)
      val bytesPerReducer = conf.getLong(BYTES_PER_REDUCER_PARAM, DEFAULT_BYTES_PER_REDUCER)

      LOG.info("Bytes/reducer: " + bytesPerReducer)

      val nReducers = math.max(1, math.ceil(totalBytes.toDouble / bytesPerReducer).toInt)

      LOG.info("Set reducers = " + nReducers)
      conf.setNumReduceTasks(nReducers)
    } else {
      LOG.info("Unable to estimate reducers; not all sizes known.")
    }
  }
}
