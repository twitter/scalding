package com.twitter.scalding.reducer_estimation

import cascading.flow.{ Flow, FlowStep, FlowStepStrategy }
import com.twitter.algebird.Monoid
import com.twitter.scalding.estimation.{ Estimator, FallbackEstimatorMonoid, FlowStrategyInfo }
import com.twitter.scalding.{ Config, StringUtility }
import java.util.{ List => JList }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

object ReducerEstimatorStepStrategy extends FlowStepStrategy[JobConf] {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  implicit val estimatorMonoid: Monoid[Estimator[Int]] =
    new FallbackEstimatorMonoid[Int]

  /**
   * Make reducer estimate, possibly overriding explicitly-set numReducers,
   * and save useful info (such as the default & estimate) in JobConf for
   * later consumption.
   *
   * Called by Cascading at the start of each job step.
   */
  final override def apply(
    flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig
    // for steps with reduce phase, mapred.reduce.tasks is set in the jobconf at this point
    // so we check that to determine if this is a map-only step.
    conf.getNumReduceTasks match {
      case 0 => LOG.info(s"${flow.getName} is a map-only step. Skipping reducer estimation.")
      case _ =>
        if (skipReducerEstimation(step)) {
          LOG.info(
            s"""
               |Flow step ${step.getName} was configured with reducers
               |set explicitly (${Config.WithReducersSetExplicitly}=true) and the estimator
               |explicit override turned off (${Config.ReducerEstimatorOverride}=false). Skipping
               |reducer estimation.
             """.stripMargin)
        } else {
          estimate(flow, preds.asScala, step)
        }
    }
  }

  // whether the reducers have been set explicitly with `withReducers`
  private def reducersSetExplicitly(step: FlowStep[JobConf]) =
    step.getConfig.getBoolean(Config.WithReducersSetExplicitly, false)

  // whether we should override explicitly-specified numReducers
  private def overrideExplicitReducers(step: FlowStep[JobConf]) =
    step.getConfig.getBoolean(Config.ReducerEstimatorOverride, false)

  private def skipReducerEstimation(step: FlowStep[JobConf]) =
    reducersSetExplicitly(step) && !overrideExplicitReducers(step)

  private def estimate(
    flow: Flow[JobConf],
    preds: Seq[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig

    val stepNumReducers = conf.get(Config.HadoopNumReducers)
    Option(conf.get(Config.ReducerEstimators)).foreach { clsNames =>

      val clsLoader = Thread.currentThread.getContextClassLoader

      val estimators = StringUtility.fastSplit(clsNames, ",")
        .map(clsLoader.loadClass(_).newInstance.asInstanceOf[Estimator[Int]])
      val combinedEstimator = Monoid.sum(estimators)

      val info = FlowStrategyInfo(flow, preds, step)

      // get estimate
      val estimatedNumReducers = combinedEstimator.estimate(info)

      // apply cap if needed
      val cappedNumReducers = estimatedNumReducers.map { n =>
        val configuredMax = conf.getInt(ReducerEstimatorConfig.maxEstimatedReducersKey, ReducerEstimatorConfig.defaultMaxEstimatedReducers)

        if (n > configuredMax) {
          LOG.warn(
            s"""
               |Reducer estimator estimated $n reducers, which is more than the configured maximum of $configuredMax.
               |Will use $configuredMax instead.
             """.stripMargin)
          configuredMax
        } else {
          n
        }
      }

      // save the estimate and capped estimate in the JobConf which should be saved by hRaven
      conf.setInt(ReducerEstimatorConfig.estimatedNumReducers, estimatedNumReducers.getOrElse(-1))
      conf.setInt(ReducerEstimatorConfig.cappedEstimatedNumReducersKey, cappedNumReducers.getOrElse(-1))
      // set number of reducers
      cappedNumReducers.foreach(conf.setNumReduceTasks)
      // log in JobConf what was explicitly set by 'withReducers'
      if (reducersSetExplicitly(step)) {
        conf.set(ReducerEstimatorConfig.originalNumReducers, stepNumReducers)
      }
    }
  }
}
