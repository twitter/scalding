package com.twitter.scalding.estimation.memory

import com.twitter.scalding.estimation.{ FlowStepHistory, FlowStrategyInfo, HistoryEstimator, Task }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

// Tuple(MapMemory in MB for java process and container, ReduceMemory in MB for java process and container),
// or None to keep the default.
case class MemoryEstimate(mapMemoryInMB: Option[(Long, Long)], reduceMemoryInMB: Option[(Long, Long)])

object SmoothedHistoryMemoryEstimator {
  val CommittedHeapBytes = "COMMITTED_HEAP_BYTES"
  val CpuMs = "CPU_MILLISECONDS"
  val PhysicalMemoryBytes = "PHYSICAL_MEMORY_BYTES"
  val GCTimeMs = "GC_TIME_MILLIS"

  implicit class MemoryRichTask(val task: Task) extends AnyVal {
    def committedHeapBytes: Option[Long] = task.counters.get(CommittedHeapBytes)
  }
}

trait SmoothedHistoryMemoryEstimator extends HistoryEstimator[MemoryEstimate] {
  import SmoothedHistoryMemoryEstimator.MemoryRichTask

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def maxHistoryItems(conf: JobConf): Int = MemoryEstimatorConfig.getMaxHistory(conf)

  override protected def estimate(info: FlowStrategyInfo, conf: JobConf, history: Seq[FlowStepHistory]): Option[MemoryEstimate] = {
    // iterate over mem history
    // collect: for maps, list of max memory in past runs
    //          for reduce, list of max memory in past runs
    // compute smoothed memory est
    // multiple by scale factor
    // return
    val maxMemory = history.map(historyMemory)

    val xmxMemoryOfMapper = xmxMemory(maxMemory.flatMap(_._1), conf)
    val xmxMemoryOfReducer = xmxMemory(maxMemory.flatMap(_._2), conf)

    val containerMemoryOfMapper = containerMemory(xmxMemoryOfMapper, conf)
    val containerMemoryOfReducer = containerMemory(xmxMemoryOfReducer, conf)

    Some(MemoryEstimate(
      cappedMemory(containerMemoryOfMapper, conf),
      cappedMemory(containerMemoryOfReducer, conf)))
  }

  private def xmxMemory(historyMemory: Seq[Long], conf: JobConf): Double = {
    val scaleFactor = MemoryEstimatorConfig.getScaleFactor(conf)
    val alpha = MemoryEstimatorConfig.getAlpha(conf)

    val smoothEstimation = smoothedAverage(historyMemory, alpha)
    val scaledEstimation = smoothEstimation * scaleFactor

    //TODO handle gc

    LOG.info(s"Calculated xmx memory for: $historyMemory smoothAvg = $smoothEstimation, scaled: $scaledEstimation")

    scaledEstimation / (1024L * 1024)
  }

  private def containerMemory(xmxMemory: Double, conf: JobConf): Double = {
    xmxMemory * MemoryEstimatorConfig.getXmxScaleFactor(conf)
  }

  private def cappedMemory(containerMemory: Double, conf: JobConf): Option[(Long, Long)] = {
    val schedulerIncrement = MemoryEstimatorConfig.getYarnSchedulerIncrement(conf)
    val roundedContainerMemory = roundUp(containerMemory, schedulerIncrement)

    val maxContainerMemory = MemoryEstimatorConfig.getMaxContainerMemory(conf)
    val minContainerMemory = MemoryEstimatorConfig.getMinContainerMemory(conf)
    val scaleFactor = MemoryEstimatorConfig.getXmxScaleFactor(conf)

    if (roundedContainerMemory == 0) {
      None
    } else if (roundedContainerMemory > maxContainerMemory) {
      Some(((maxContainerMemory / scaleFactor).toLong, maxContainerMemory))
    } else if (roundedContainerMemory < minContainerMemory) {
      Some(((minContainerMemory / scaleFactor).toLong, minContainerMemory))
    } else {
      Some((roundedContainerMemory / scaleFactor).toLong, roundedContainerMemory)
    }
  }

  private def historyMemory(history: FlowStepHistory): (Option[Long], Option[Long]) = {
    LOG.debug(s"Processing tasks: ${history.tasks}")
    val reduceTasks: Seq[Task] = history.tasks.filter { t => t.taskType.contains("REDUCE") }
    val mapTasks: Seq[Task] = history.tasks.filter { t => t.taskType.contains("MAP") }

    // handle empty task list due to either no task history / lack of reducers
    val maxReduceCommittedHeap: Option[Long] =
      if (reduceTasks.isEmpty)
        None
      else
        Some(reduceTasks.flatMap(_.committedHeapBytes).max)

    val maxMapCommittedHeap: Option[Long] =
      if (mapTasks.isEmpty)
        None
      else
        Some(mapTasks.flatMap(_.committedHeapBytes).max)

    LOG.info(s"Calculated max committed heap for job: ${history.keys}, map: $maxMapCommittedHeap reduce: $maxReduceCommittedHeap")
    (maxMapCommittedHeap, maxReduceCommittedHeap)
  }

  // memoryEstimate = (currentMemoryValue * alpha) + (1 - alpha) * oldEstimate
  private def smoothedAverage(memoryList: Seq[Long], alpha: Double): Double =
    memoryList
      .foldLeft(0.0) { (oldEstimate, currentVal) =>
        (currentVal * alpha) + (1 - alpha) * oldEstimate
      }

  private def roundUp(value: Double, block: Double): Long =
    (Math.ceil(value / block) * block).toLong
}