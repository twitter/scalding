package com.twitter.scalding.hraven.estimation.memory

import com.twitter.hraven.{ CounterMap, TaskDetails }
import com.twitter.scalding.estimation.Task
import com.twitter.scalding.estimation.memory.SmoothedHistoryMemoryEstimator
import com.twitter.scalding.hraven.estimation.HRavenHistoryService

trait HRavenMemoryHistoryService extends HRavenHistoryService {
  import SmoothedHistoryMemoryEstimator._

  private val TaskCounterGroup = "org.apache.hadoop.mapreduce.TaskCounter"

  override protected val detailFields: List[String] = List(Task.TaskType)
  override protected val counterFields: List[String] = List(
    "org.apache.hadoop.mapreduce.TaskCounter.COMMITTED_HEAP_BYTES",
    "org.apache.hadoop.mapreduce.TaskCounter.PHYSICAL_MEMORY_BYTES",
    "org.apache.hadoop.mapreduce.TaskCounter.GC_TIME_MILLIS",
    "org.apache.hadoop.mapreduce.TaskCounter.CPU_MILLISECONDS")

  override protected def details(taskDetails: TaskDetails): Option[Map[String, Any]] = {
    if (taskDetails.getType.nonEmpty) {
      Some(Map(Task.TaskType -> taskDetails.getType))
    } else {
      None
    }
  }

  override protected def counters(taskCounters: CounterMap): Option[Map[String, Long]] = {
    //sometimes get groups with only partial data
    if (taskCounters.getGroups.isEmpty || taskCounters.getGroup(TaskCounterGroup).size() < 4) {
      None
    } else {
      val group = taskCounters.getGroup(TaskCounterGroup)

      Some(Map(
        CommittedHeapBytes -> group.get(CommittedHeapBytes).getValue,
        CpuMs -> group.get(CpuMs).getValue,
        PhysicalMemoryBytes -> group.get(PhysicalMemoryBytes).getValue,
        GCTimeMs -> group.get(GCTimeMs).getValue))
    }
  }
}

object HRavenMemoryHistoryService extends HRavenMemoryHistoryService

class HRavenSmoothedMemoryEstimator extends SmoothedHistoryMemoryEstimator {
  override def historyService = HRavenMemoryHistoryService
}
