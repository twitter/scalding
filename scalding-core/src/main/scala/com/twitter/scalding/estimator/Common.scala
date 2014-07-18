package com.twitter.scalding.estimator

import cascading.flow.{ FlowStep, Flow, FlowStepStrategy }
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList }

import org.slf4j.{ LoggerFactory, Logger }

object EstimatorConfig {

  /** Name of parameter to specify which class to use as the default estimator. */
  val reducerEstimator = "scalding.reducer.estimator"

  /** Name of parameter to specify which class to use as the default estimator. */
  val reducerEstimate = "scalding.reducer.estimator.result"

  /** Whether estimator should override manually-specified reducers. */
  val reducerEstimatorOverride = "scalding.reducer.estimator.override"

  /** What the original job config was. */
  val reducerExplicit = "scalding.reducer.estimator.explicit"

  /** Hadoop numReducers param */
  val numReducers = "mapred.reduce.tasks"

}

class ReducerEstimator extends FlowStepStrategy[JobConf] {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * Override this to customize reducer estimator.
   */
  def reducerEstimate(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Option[Int] = None

  override def apply(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Unit = {
    val conf = flowStep.getConfig

    val flowNumReducers = flow.getConfig.get(EstimatorConfig.numReducers)
    val stepNumReducers = conf.get(EstimatorConfig.numReducers)
    val setExplicitly = flowNumReducers != stepNumReducers

    // log in JobConf what was explicitly set by 'withReducers'
    if (setExplicitly) conf.set(EstimatorConfig.reducerExplicit, stepNumReducers)

    // whether we should override explicitly-specified numReducers
    val overrideExplicit = conf.getBoolean(EstimatorConfig.reducerEstimatorOverride, false)

    val numReducers = reducerEstimate(flow, predecessorSteps, flowStep).getOrElse(0)

    // save the estimate in the JobConf which should be saved by hRaven
    conf.setInt(EstimatorConfig.reducerEstimate, numReducers)

    if (numReducers > 0 && (!setExplicitly || overrideExplicit)) {
      // set number of reducers
      conf.setNumReduceTasks(numReducers)
    }
  }
}
