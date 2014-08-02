package com.twitter.scalding.reducer_estimation

import cascading.flow.{ FlowStep, Flow, FlowStepStrategy }
import com.twitter.scalding.Config
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList }

import org.slf4j.{ LoggerFactory, Logger }

object EstimatorConfig {

  /** Output param: what the Reducer Estimator recommended, regardless of if it was used. */
  val estimatedNumReducers = "scalding.reducer.estimator.result"

  /** Output param: what the original job config was. */
  val originalNumReducers = "scalding.reducer.estimator.original.mapred.reduce.tasks"

}

class ReducerEstimator extends FlowStepStrategy[JobConf] {

  /**
   * Estimate how many reducers should be used. Called for each FlowStep before
   * it is scheduled. Custom reducer estimators should override this rather than
   * apply() directly.
   *
   * @param flow              Information about the overall flow
   * @param predecessorSteps  Already-run steps in the flow
   * @param flowStep          Current flow step. Changes should be made to this one.
   *
   * @return Number of reducers recommended by the estimator, or None to keep the default.
   */
  def estimateReducers(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Option[Int] = None

  /**
   * Make reducer estimate, possibly overriding explicitly-set numReducers,
   * and save useful info (such as the default & estimate) in JobConf for
   * later consumption.
   *
   * Called by Cascading at the start of each job step.
   */
  final override def apply(flow: Flow[JobConf],
    predecessorSteps: JList[FlowStep[JobConf]],
    flowStep: FlowStep[JobConf]): Unit = {
    val conf = flowStep.getConfig

    val flowNumReducers = flow.getConfig.get(Config.HadoopNumReducers)
    val stepNumReducers = conf.get(Config.HadoopNumReducers)

    // assuming that if the step's reducers is different than the default for the flow,
    // it was probably set by `withReducers` explicitly. This isn't necessarily true --
    // Cascading may have changed it for its own reasons.
    // TODO: disambiguate this by setting something in JobConf when `withReducers` is called
    val setExplicitly = flowNumReducers != stepNumReducers

    // log in JobConf what was explicitly set by 'withReducers'
    if (setExplicitly) conf.set(EstimatorConfig.originalNumReducers, stepNumReducers)

    // whether we should override explicitly-specified numReducers
    val overrideExplicit = conf.getBoolean(Config.ReducerEstimatorOverride, false)

    // make estimate, making 'None' -> 0 so we can log it
    val numReducers = estimateReducers(flow, predecessorSteps, flowStep).getOrElse(0)

    // save the estimate in the JobConf which should be saved by hRaven
    conf.setInt(EstimatorConfig.estimatedNumReducers, numReducers)

    if (numReducers > 0 && (!setExplicitly || overrideExplicit)) {
      // set number of reducers
      conf.setNumReduceTasks(numReducers)
    }
  }
}
