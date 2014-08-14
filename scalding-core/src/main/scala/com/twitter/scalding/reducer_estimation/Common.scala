package com.twitter.scalding.reducer_estimation

import cascading.flow.{ FlowStep, Flow, FlowStepStrategy }
import com.twitter.algebird.Monoid
import com.twitter.scalding.Config
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList }

import scala.collection.JavaConverters._
import scala.util.Try

object EstimatorConfig {

  /** Output param: what the Reducer Estimator recommended, regardless of if it was used. */
  val estimatedNumReducers = "scalding.reducer.estimator.result"

  /** Output param: what the original job config was. */
  val originalNumReducers = "scalding.reducer.estimator.original.mapred.reduce.tasks"

  /** Maximum number of history items to use for reducer estimation. */
  val maxHistoryKey = "scalding.reducer.estimator.max.history"

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 1)

}

case class FlowStrategyInfo(
  flow: Flow[JobConf],
  predecessorSteps: Seq[FlowStep[JobConf]],
  step: FlowStep[JobConf])

class ReducerEstimator {
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

}

case class FallbackEstimator(first: ReducerEstimator, fallback: ReducerEstimator) extends ReducerEstimator {
  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    first.estimateReducers(info) orElse fallback.estimateReducers(info)
}

object ReducerEstimatorStepStrategy extends FlowStepStrategy[JobConf] {

  implicit val estimatorMonoid: Monoid[ReducerEstimator] = new Monoid[ReducerEstimator] {
    override def zero: ReducerEstimator = new ReducerEstimator
    override def plus(l: ReducerEstimator, r: ReducerEstimator): ReducerEstimator =
      FallbackEstimator(l, r)
  }

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

    val flowNumReducers = flow.getConfig.get(Config.HadoopNumReducers)
    val stepNumReducers = conf.get(Config.HadoopNumReducers)

    // assuming that if the step's reducers is different than the default for the flow,
    // it was probably set by `withReducers` explicitly. This isn't necessarily true --
    // Cascading may have changed it for its own reasons.
    // TODO: disambiguate this by setting something in JobConf when `withReducers` is called
    // (will be addressed by https://github.com/twitter/scalding/pull/973)
    val setExplicitly = flowNumReducers != stepNumReducers

    // log in JobConf what was explicitly set by 'withReducers'
    if (setExplicitly) conf.set(EstimatorConfig.originalNumReducers, stepNumReducers)

    // whether we should override explicitly-specified numReducers
    val overrideExplicit = conf.getBoolean(Config.ReducerEstimatorOverride, false)

    Option(conf.get(Config.ReducerEstimators)).map { clsNames =>

      val clsLoader = Thread.currentThread.getContextClassLoader

      val estimators = clsNames.split(",")
        .map(clsLoader.loadClass(_).newInstance.asInstanceOf[ReducerEstimator])
      val combinedEstimator = Monoid.sum(estimators)

      // try to make estimate
      val info = FlowStrategyInfo(flow, preds.asScala, step)

      // if still None, make it '-1' to make it simpler to log
      val numReducers = combinedEstimator.estimateReducers(info)

      // save the estimate in the JobConf which should be saved by hRaven
      conf.setInt(EstimatorConfig.estimatedNumReducers, numReducers.getOrElse(-1))

      // set number of reducers
      if (!setExplicitly || overrideExplicit) {
        numReducers.foreach(conf.setNumReduceTasks)
      }
    }
  }
}

/**
 * Info about a prior FlowStep, provided by implementers of HistoryService
 */
sealed trait FlowStepHistory {
  /** Size of input to mappers (in bytes) */
  def mapperBytes: Long
  /** Size of input to reducers (in bytes) */
  def reducerBytes: Long
}

object FlowStepHistory {
  def apply(m: Long, r: Long) = new FlowStepHistory {
    override def mapperBytes: Long = m
    override def reducerBytes: Long = r
  }
}

/**
 * Provider of information about prior runs.
 */
trait HistoryService {
  /**
   * Retrieve history for matching FlowSteps, up to `max`
   */
  def fetchHistory(f: FlowStep[JobConf], max: Int): Try[Seq[FlowStepHistory]]
}
