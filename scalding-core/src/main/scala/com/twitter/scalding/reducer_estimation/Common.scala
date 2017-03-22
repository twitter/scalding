package com.twitter.scalding.reducer_estimation

import cascading.flow.{ Flow, FlowStep, FlowStepStrategy }
import cascading.tap.hadoop.Hfs
import cascading.tap.{ CompositeTap, Tap }
import com.twitter.algebird.Monoid
import com.twitter.scalding.tap.GlobHfs
import com.twitter.scalding.{ Config, StringUtility }
import java.util.{ List => JList }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

object EstimatorConfig {

  /** Output param: what the Reducer Estimator recommended, regardless of if it was used. */
  val estimatedNumReducers = "scalding.reducer.estimator.result"

  /**
   * Output param: same as estimatedNumReducers but with the cap specified by maxEstimatedReducersKey
   * applied. Can be used to determine whether a cap was applied to the estimated number of reducers
   * and potentially to trigger alerting / logging.
   */
  val cappedEstimatedNumReducersKey = "scalding.reducer.estimator.result.capped"

  /** Output param: what the original job config was. */
  val originalNumReducers = "scalding.reducer.estimator.original.mapred.reduce.tasks"

  /** Maximum number of history items to use for reducer estimation. */
  val maxHistoryKey = "scalding.reducer.estimator.max.history"

  /**
   * If we estimate more than this number of reducers,
   * we will use this number instead of the estimated value
   */
  val maxEstimatedReducersKey = "scalding.reducer.estimator.max.estimated.reducers"

  /* fairly arbitrary choice here -- you will probably want to configure this in your cluster defaults */
  val defaultMaxEstimatedReducers = 5000

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 1)

}

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

case class FlowStrategyInfo(
  flow: Flow[JobConf],
  predecessorSteps: Seq[FlowStep[JobConf]],
  step: FlowStep[JobConf])

trait ReducerEstimator {
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
  def estimateReducers(info: FlowStrategyInfo): Option[Int]

}

trait HistoryReducerEstimator extends ReducerEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def historyService: HistoryService

  override def estimateReducers(info: FlowStrategyInfo): Option[Int] = {
    val conf = info.step.getConfig
    val maxHistory = EstimatorConfig.getMaxHistory(conf)

    historyService.fetchHistory(info, maxHistory) match {
      case Success(h) if h.isEmpty =>
        LOG.warn("No matching history found.")
        None
      case Success(h) =>
        LOG.info(s"${h.length} history entries found.")
        val estimate = estimateReducers(info, h)
        LOG.info(s"Reducer estimate: $estimate")
        estimate
      case Failure(f) =>
        LOG.warn(s"Unable to fetch history in $getClass", f)
        None
    }
  }

  protected def estimateReducers(info: FlowStrategyInfo, history: Seq[FlowStepHistory]): Option[Int]
}

case class FallbackEstimator(first: ReducerEstimator, fallback: ReducerEstimator) extends ReducerEstimator {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def estimateReducers(info: FlowStrategyInfo): Option[Int] =
    first.estimateReducers(info).orElse {
      LOG.warn(s"$first estimator failed. Falling back to $fallback.")
      fallback.estimateReducers(info)
    }
}

object ReducerEstimatorStepStrategy extends FlowStepStrategy[JobConf] {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  implicit val estimatorMonoid: Monoid[ReducerEstimator] = new Monoid[ReducerEstimator] {
    override def zero: ReducerEstimator = new ReducerEstimator {
      override def estimateReducers(info: FlowStrategyInfo) = None
    }

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
          estimate(flow, preds, step)
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

  private def estimate(flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig

    val stepNumReducers = conf.get(Config.HadoopNumReducers)
    Option(conf.get(Config.ReducerEstimators)).foreach { clsNames =>

      val clsLoader = Thread.currentThread.getContextClassLoader

      val estimators = StringUtility.fastSplit(clsNames, ",")
        .map(clsLoader.loadClass(_).newInstance.asInstanceOf[ReducerEstimator])
      val combinedEstimator = Monoid.sum(estimators)

      val info = FlowStrategyInfo(flow, preds.asScala, step)

      // get estimate
      val estimatedNumReducers = combinedEstimator.estimateReducers(info)

      // apply cap if needed
      val cappedNumReducers = estimatedNumReducers.map { n =>
        val configuredMax = conf.getInt(EstimatorConfig.maxEstimatedReducersKey, EstimatorConfig.defaultMaxEstimatedReducers)

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
      conf.setInt(EstimatorConfig.estimatedNumReducers, estimatedNumReducers.getOrElse(-1))
      conf.setInt(EstimatorConfig.cappedEstimatedNumReducersKey, cappedNumReducers.getOrElse(-1))
      // set number of reducers
      cappedNumReducers.foreach(conf.setNumReduceTasks)
      // log in JobConf what was explicitly set by 'withReducers'
      if (reducersSetExplicitly(step)) {
        conf.set(EstimatorConfig.originalNumReducers, stepNumReducers)
      }
    }
  }
}

/**
 * Info about a prior FlowStep, provided by implementers of HistoryService
 */
final case class FlowStepHistory(keys: FlowStepKeys,
  submitTime: Long,
  launchTime: Long,
  finishTime: Long,
  totalMaps: Long,
  totalReduces: Long,
  finishedMaps: Long,
  finishedReduces: Long,
  failedMaps: Long,
  failedReduces: Long,
  mapFileBytesRead: Long,
  mapFileBytesWritten: Long,
  mapOutputBytes: Long,
  reduceFileBytesRead: Long,
  hdfsBytesRead: Long,
  hdfsBytesWritten: Long,
  mapperTimeMillis: Long,
  reducerTimeMillis: Long,
  reduceShuffleBytes: Long,
  cost: Double,
  tasks: Seq[Task])

final case class FlowStepKeys(jobName: String,
  user: String,
  priority: String,
  status: String,
  version: String,
  queue: String)

final case class Task(
  taskType: String,
  status: String,
  startTime: Long,
  finishTime: Long)

/**
 * Provider of information about prior runs.
 */
trait HistoryService {
  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]]
}
