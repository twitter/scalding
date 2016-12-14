package com.twitter.scalding.memory_estimation

import cascading.flow.{ Flow, FlowStep, FlowStepStrategy }
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ Config, StringUtility }
import com.twitter.scalding.reducer_estimation.{ FlowStepKeys, FlowStrategyInfo }
import java.util.{ List => JList }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

/**
 * Info about a prior FlowStep, provided by implementers of HistoryService
 */
final case class FlowStepMemoryHistory(keys: FlowStepKeys,
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

final case class Task(
  taskType: String,
  committedHeapBytes: Long,
  physicalMemoryBytes: Long,
  cpuMilliSeconds: Long,
  gcTimeMillis: Long)

/**
 * Provider of information about prior runs.
 */
trait MemoryService {
  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepMemoryHistory]]
}

object MemoryEstimatorConfig {

  /** Output param: what the original job map memory was. */
  val originalMapMemory = "scalding.map.memory.estimator.original"

  /** Output param: what the original job map memory was. */
  val originalReduceMemory = "scalding.reduce.memory.estimator.original"

  /** Maximum number of history items to use for memory estimation. */
  val maxHistoryKey = "scalding.memory.estimator.max.history"

  /**
   * Value of alpha for exponential smoothing.
   * Lower values ensure more smoothing and less importance to newer data
   * Higher values provide lesser smoothing and more importance to newer data
   */
  val alphaKey = "scalding.memory.estimator.alpha"

  /** Indicates how much to scale the memory estimate after it's calculated */
  val memoryScaleFactor = "scalding.memory.scale.factor"

  //yarn allocates in increments. So we might as well round up our container ask
  val yarnSchedulerIncrementAllocationMB: String = "yarn.scheduler.increment-allocation-mb"

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 5)

  def getAlpha(conf: JobConf): Double = conf.getDouble(alphaKey, 1.0)

  def getScaleFactor(conf: JobConf): Double = conf.getDouble(memoryScaleFactor, 1.2)

  def getYarnSchedulerIncrement(conf: JobConf): Int = conf.getInt(yarnSchedulerIncrementAllocationMB, 512)

  // max container is 8GB, given XmxToMemoryScaleFactor we want to ensure this
  // estimate when multiplied by that stays below 8GB
  val maxMemoryEstimate: Double = 6500.0 * 1024 * 1024

  // min container is 1G, multiplying by XmxToMemoryScaleFactor keeps us under the
  // min container size
  val minMemoryEstimate: Double = 800.0 * 1024 * 1024

  val XmxToMemoryScaleFactor: Double = 1.25

}

// Tuple(MapMemory in MB, ReduceMemory in MB), or None to keep the default.
case class MemoryEstimate(mapMemory: Option[Long], reduceMemory: Option[Long])

trait MemoryEstimator {
  /**
   * Estimate Map / Reduce memory settings. Called for each FlowStep before
   * it is scheduled. Custom memory estimators should override this rather than
   * apply() directly.
   *
   * @param info  Holds information about the overall flow (.flow),
   *              previously-run steps (.predecessorSteps),
   *              and the current step (.step).
   * @return MemoryEstimate.
   */
  def estimateMemory(info: FlowStrategyInfo): Option[MemoryEstimate]
}

trait HistoryMemoryEstimator extends MemoryEstimator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def memoryService: MemoryService

  override def estimateMemory(info: FlowStrategyInfo): Option[MemoryEstimate] = {
    val conf = info.step.getConfig
    val maxHistory = MemoryEstimatorConfig.getMaxHistory(conf)
    val alpha = MemoryEstimatorConfig.getAlpha(conf)
    val scaleFactor = MemoryEstimatorConfig.getScaleFactor(conf)
    val yarnSchedulerIncrementMB = MemoryEstimatorConfig.getYarnSchedulerIncrement(conf)

    LOG.info(s"Attempting to estimate memory settings with maxHistory: $maxHistory, alpha: $alpha, scaleFactor: $scaleFactor, schedulerIncrement: $yarnSchedulerIncrementMB")

    memoryService.fetchHistory(info, maxHistory) match {
      case Success(h) if h.isEmpty =>
        LOG.warn("No matching history found.")
        None
      case Success(h) =>
        LOG.info(s"${h.length} history entries found.")
        val estimate = estimateMemory(info, h, alpha, scaleFactor)
        LOG.info(s"Memory estimate: $estimate")
        Some(estimate)
      case Failure(f) =>
        LOG.warn(s"Unable to fetch history in $getClass", f)
        None
    }
  }

  private def estimateMemory(info: FlowStrategyInfo, history: Seq[FlowStepMemoryHistory], alpha: Double, scaleFactor: Double): MemoryEstimate = {
    // iterate over mem history
    // collect: for maps, list of max memory in past runs
    //          for reduce, list of max memory in past runs
    // compute smoothed memory est
    // multiple by scale factor
    // cap estimate to max size if needed
    // handle gc
    // return
    val maxMemoryList: Seq[(Option[Long], Option[Long])] = history.map{ h => getMapReduceMemory(h) }
    val maxMapList: Seq[Long] = maxMemoryList.flatMap(_._1)
    val maxReduceList: Seq[Long] = maxMemoryList.flatMap(_._2)

    val mapSmoothEst: Double = smoothedAverage(maxMapList, alpha)
    val reduceSmoothEst: Double = smoothedAverage(maxReduceList, alpha)

    val mapScaledEst = mapSmoothEst * scaleFactor
    val reduceScaledEst = reduceSmoothEst * scaleFactor

    //todo handle gc values
    val cappedMapEst = cappedMemoryEstimateMB(mapScaledEst)
    val cappedReduceEst = cappedMemoryEstimateMB(reduceScaledEst)

    LOG.info(s"Calculated map val for: $maxMapList smoothAvg = $mapSmoothEst, scaled: $mapScaledEst, capped: $cappedMapEst")
    LOG.info(s"Calculated reduce val for: $maxReduceList smoothAvg = $reduceSmoothEst, scaled: $reduceScaledEst, capped: $cappedReduceEst")

    MemoryEstimate(cappedMapEst, cappedReduceEst)
  }

  private def getMapReduceMemory(history: FlowStepMemoryHistory): (Option[Long], Option[Long]) = {
    LOG.info(s"Processing tasks: ${history.tasks}")
    val reduceTasks: Seq[Task] = history.tasks.filter { t => t.taskType == "REDUCE" }
    val mapTasks: Seq[Task] = history.tasks.filter { t => t.taskType == "MAP" }

    // handle empty task list due to either no task history / lack of reducers
    val maxReduceCommittedHeap: Option[Long] = if (reduceTasks.isEmpty) None else Some(reduceTasks.map(_.committedHeapBytes).max)
    val maxMapCommittedHeap: Option[Long] = if (mapTasks.isEmpty) None else Some(mapTasks.map(_.committedHeapBytes).max)

    LOG.info(s"Calculated max committed heap for job: ${history.keys}, map: $maxMapCommittedHeap reduce: $maxReduceCommittedHeap")
    (maxMapCommittedHeap, maxReduceCommittedHeap)
  }

  // memoryEstimate = (currentMemoryValue * alpha) + (1 - alpha) * oldEstimate
  private def smoothedAverage(memoryList: Seq[Long], alpha: Double): Double = {
    memoryList.foldLeft(0.0){ (oldEstimate, currentVal) => (currentVal * alpha) + (1 - alpha) * oldEstimate }
  }

  // calculate the capped Xmx memory estimate
  private def cappedMemoryEstimateMB(memoryEst: Double): Option[Long] = {
    val memoryEstimateBytes: Option[Double] =
      if (memoryEst > MemoryEstimatorConfig.maxMemoryEstimate)
        Some(MemoryEstimatorConfig.maxMemoryEstimate)
      else if (memoryEst == 0)
        None
      else if (memoryEst < MemoryEstimatorConfig.minMemoryEstimate)
        Some(MemoryEstimatorConfig.minMemoryEstimate)
      else
        Some(memoryEst)

    memoryEstimateBytes.map{ est => (est / (1024 * 1024)).toLong }
  }

}

case class FallbackMemoryEstimator(first: MemoryEstimator, fallback: MemoryEstimator) extends MemoryEstimator {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def estimateMemory(info: FlowStrategyInfo): Option[MemoryEstimate] =
    first.estimateMemory(info).orElse {
      LOG.warn(s"$first estimator failed. Falling back to $fallback.")
      fallback.estimateMemory(info)
    }
}

object MemoryEstimatorStepStrategy extends FlowStepStrategy[JobConf] {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  implicit val estimatorMonoid: Monoid[MemoryEstimator] = new Monoid[MemoryEstimator] {
    override def zero: MemoryEstimator = new MemoryEstimator {
      override def estimateMemory(info: FlowStrategyInfo): Option[MemoryEstimate] = None
    }

    override def plus(l: MemoryEstimator, r: MemoryEstimator): MemoryEstimator =
      FallbackMemoryEstimator(l, r)
  }

  /**
   * Make memory estimate, possibly overriding explicitly-set memory settings,
   * and save useful info (such as the default & estimate) in JobConf for
   * later consumption.
   *
   * Called by Cascading at the start of each job step.
   */
  final override def apply(flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {

    if (skipMemoryEstimation(step)) {
      LOG.info(s"Skipping memory estimation as ${Config.MemoryEstimators} is not set ")
    } else {
      estimate(flow, preds, step)
    }
  }

  private def skipMemoryEstimation(step: FlowStep[JobConf]): Boolean =
    step.getConfig.get(Config.MemoryEstimators) == null

  private def estimate(flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig

    Option(conf.get(Config.MemoryEstimators)).foreach { clsNames =>

      val clsLoader = Thread.currentThread.getContextClassLoader

      val estimators = StringUtility.fastSplit(clsNames, ",")
        .map(clsLoader.loadClass(_).newInstance.asInstanceOf[MemoryEstimator])
      val combinedEstimator = Monoid.sum(estimators)

      val info = FlowStrategyInfo(flow, preds.asScala, step)

      // get memory estimate
      val memoryEstimate: Option[MemoryEstimate] = combinedEstimator.estimateMemory(info)

      memoryEstimate match {
        case Some(MemoryEstimate(Some(mapMem), Some(reduceMem))) =>
          LOG.info(s"Overriding map Xmx memory to: $mapMem and reduce Xmx memory to $reduceMem")
          setMapMemory(mapMem, conf)
          setReduceMemory(reduceMem, conf)
        case Some(MemoryEstimate(Some(mapMem), _)) =>
          LOG.info(s"Overriding only map Xmx memory to: $mapMem")
          setMapMemory(mapMem, conf)
        case Some(MemoryEstimate(_, Some(reduceMem))) =>
          LOG.info(s"Overriding only reduce Xmx memory to $reduceMem")
          setReduceMemory(reduceMem, conf)
        case _ => LOG.info("Memory estimators didn't calculate any value. Skipping setting memory overrides")
      }
    }
  }

  private def setMapMemory(mapMem: Long, conf: Configuration): Unit = {
    conf.setLong(MemoryEstimatorConfig.originalMapMemory, conf.getLong(Config.MapMemory, 0L))

    val mapContainerMem = mapMem * MemoryEstimatorConfig.XmxToMemoryScaleFactor
    val schedulerIncrement = conf.getInt(MemoryEstimatorConfig.yarnSchedulerIncrementAllocationMB, 512)
    val roundedMapContainerMem = roundUp(mapContainerMem, schedulerIncrement).toLong

    conf.setLong(Config.MapMemory, roundedMapContainerMem)

    val mapOpts = conf.get(Config.MapJavaOpts, "")
    //remove existing xmx / xms
    val mapOptsWithoutXm = mapOpts.split(" ").filterNot(s => s.startsWith("-Xmx") || s.startsWith("-Xms")).mkString(" ")

    conf.set(Config.MapJavaOpts, mapOptsWithoutXm + s" -Xmx${mapMem}M")
  }

  private def setReduceMemory(reduceMem: Long, conf: Configuration): Unit = {
    conf.setLong(MemoryEstimatorConfig.originalReduceMemory, conf.getLong(Config.ReduceMemory, 0L))

    val reduceContainerMem = reduceMem * MemoryEstimatorConfig.XmxToMemoryScaleFactor
    val schedulerIncrement = conf.getInt(MemoryEstimatorConfig.yarnSchedulerIncrementAllocationMB, 512)
    val roundedReduceContainerMem = roundUp(reduceContainerMem, schedulerIncrement).toLong
    conf.setLong(Config.ReduceMemory, roundedReduceContainerMem)

    val reduceOpts = conf.get(Config.ReduceJavaOpts, "")
    //remove existing xmx / xms
    val reduceOptsWithoutXm = reduceOpts.split(" ").filterNot(s => s.startsWith("-Xmx") || s.startsWith("-Xms")).mkString(" ")

    conf.set(Config.ReduceJavaOpts, reduceOptsWithoutXm + s" -Xmx${reduceMem}M")
  }

  //Round up value to a multiple of block
  private def roundUp(value: Double, block: Double): Double = Math.ceil(value / block) * block
}
