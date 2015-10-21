package com.twitter.scalding.reducer_estimation

import cascading.flow.{ FlowStep, Flow, FlowStepStrategy }
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ StringUtility, Config }
import cascading.tap.{ Tap, CompositeTap }
import cascading.tap.hadoop.Hfs
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import java.util.{ List => JList }

import scala.collection.JavaConverters._
import scala.util.{ Try, Success, Failure }

object EstimatorConfig {

  /** Output param: what the Reducer Estimator recommended, regardless of if it was used. */
  val estimatedNumReducers = "scalding.reducer.estimator.result"

  /** Output param: what the original job config was. */
  val originalNumReducers = "scalding.reducer.estimator.original.mapred.reduce.tasks"

  /** Maximum number of history items to use for reducer estimation. */
  val maxHistoryKey = "scalding.reducer.estimator.max.history"

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 1)

}

object Common {
  private def unrollTaps(taps: Seq[Tap[_, _, _]]): Seq[Tap[_, _, _]] =
    taps.flatMap {
      case multi: CompositeTap[_] =>
        unrollTaps(multi.getChildTaps.asScala.toSeq)
      case t => Seq(t)
    }

  def unrollTaps(step: FlowStep[_ <: JobConf]): Seq[Tap[_, _, _]] =
    unrollTaps(step.getSources.asScala.toSeq)

  /**
   * Get the total size of the file(s) specified by the Hfs, which may contain a glob
   * pattern in its path, so we must be ready to handle that case.
   */
  def size(f: Hfs, conf: JobConf): Long = {
    val fs = f.getPath.getFileSystem(conf)
    fs.globStatus(f.getPath)
      .map{ s => fs.getContentSummary(s.getPath).getLength }
      .sum
  }

  def inputSizes(step: FlowStep[JobConf]): Seq[(String, Long)] = {
    val conf = step.getConfig
    unrollTaps(step).flatMap {
      case tap: Hfs => Some(tap.toString -> size(tap, conf))
      case _ => None
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
        LOG.info(s"Reducer estimate: ${estimate}")
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
      case _ => estimate(flow, preds, step)
    }
  }

  private def estimate(flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig
    val stepNumReducers = conf.get(Config.HadoopNumReducers)

    // whether the reducers have been set explicitly with `withReducers`
    val setExplicitly = conf.getBoolean(Config.WithReducersSetExplicitly, false)

    // log in JobConf what was explicitly set by 'withReducers'
    if (setExplicitly) conf.set(EstimatorConfig.originalNumReducers, stepNumReducers)

    // whether we should override explicitly-specified numReducers
    val overrideExplicit = conf.getBoolean(Config.ReducerEstimatorOverride, false)

    Option(conf.get(Config.ReducerEstimators)).map { clsNames =>

      val clsLoader = Thread.currentThread.getContextClassLoader

      val estimators = StringUtility.fastSplit(clsNames, ",")
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
