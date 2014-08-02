package com.twitter.scalding.reducer_estimation

import cascading.flow.{ FlowStep, Flow, FlowStepStrategy }
import com.twitter.scalding.Config
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList }

import org.slf4j.{ LoggerFactory, Logger }

object EstimatorConfig {

  /** Output param: what the Reducer Estimator recommended, regardless of if it was used. */
  val estimatedNumReducers = "scalding.reducer.estimator.result"

  /** Whether estimator should override manually-specified reducers. */
  val reducerEstimatorOverride = "scalding.reducer.estimator.override"

  /** Output param: what the original job config was. */
  val originalNumReducers = "scalding.reducer.estimator.original.mapred.reduce.tasks"

  /**
   * Parameter that actually controls the number of reduce tasks.
   * Be sure to set this in the JobConf for the *step* not the flow.
   */
  val hadoopNumReducers = "mapred.reduce.tasks"
}

case class FlowStrategyInfo(flow: Flow[JobConf],
  predecessorSteps: JList[FlowStep[JobConf]],
  step: FlowStep[JobConf])

class ReducerEstimator extends FlowStepStrategy[JobConf] {

  /**
   * Estimate how many reducers should be used. Called for each FlowStep before
   * it is scheduled. Custom reducer estimators should override this rather than
   * apply() directly.
   *
   * @param info  Holds information about the overall flow (.flow),
   *              previously-run steps (.predecessorSteps),
   *              and the current step (.step).
   * @return Number of reducers recommended by the estimator, or None to keep the default.
   */
  def estimateReducers(info: FlowStrategyInfo): Option[Int] = None

  lazy val fallbackEstimator: Option[ReducerEstimator] = None

  /**
   * Make reducer estimate, possibly overriding explicitly-set numReducers,
   * and save useful info (such as the default & estimate) in JobConf for
   * later consumption.
   *
   * Called by Cascading at the start of each job step.
   */
  final override def apply(flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig

    val flowNumReducers = flow.getConfig.get(EstimatorConfig.hadoopNumReducers)
    val stepNumReducers = conf.get(EstimatorConfig.hadoopNumReducers)

    // assuming that if the step's reducers is different than the default for the flow,
    // it was probably set by `withReducers` explicitly. This isn't necessarily true --
    // Cascading may have changed it for its own reasons.
    // TODO: disambiguate this by setting something in JobConf when `withReducers` is called
    // (will be addressed by https://github.com/twitter/scalding/pull/973)
    val setExplicitly = flowNumReducers != stepNumReducers

    // log in JobConf what was explicitly set by 'withReducers'
    if (setExplicitly) conf.set(EstimatorConfig.originalNumReducers, stepNumReducers)

    // whether we should override explicitly-specified numReducers
    val overrideExplicit = conf.getBoolean(EstimatorConfig.reducerEstimatorOverride, false)

    // try to make estimate
    val info = FlowStrategyInfo(flow, preds, step)
    val numReducers = estimateReducers(info)
      // if estimate was None, try fallback estimator
      .getOrElse(fallbackEstimator.flatMap(_.estimateReducers(info))
        // if still None, make it '0' so we can log it
        .getOrElse(0))

    // save the estimate in the JobConf which should be saved by hRaven
    conf.setInt(EstimatorConfig.estimatedNumReducers, numReducers)

    if (numReducers > 0 && (!setExplicitly || overrideExplicit)) {
      // set number of reducers
      conf.setNumReduceTasks(numReducers)
    }
  }
}