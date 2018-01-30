package com.twitter.scalding.hraven.reducer_estimation

import com.twitter.hraven.{ CounterMap, TaskDetails }
import com.twitter.scalding.estimation.Task
import com.twitter.scalding.hraven.estimation.HRavenHistoryService
import com.twitter.scalding.reducer_estimation.{ RatioBasedEstimator, RuntimeReducerEstimator }

trait HRavenReducerHistoryService extends HRavenHistoryService {
  override protected val counterFields: List[String] = List()
  override protected val detailFields: List[String] = List(
    Task.TaskType,
    "status",
    "startTime",
    "finishTime")

  override protected def counters(taskCounters: CounterMap): Option[Map[String, Long]] = Some(Map.empty)

  override protected def details(taskDetails: TaskDetails): Option[Map[String, Any]] =
    if (taskDetails.getType.nonEmpty) {
      Some(Map(
        Task.TaskType -> taskDetails.getType,
        "status" -> taskDetails.getStatus,
        "startTime" -> taskDetails.getStartTime,
        "finishTime" -> taskDetails.getFinishTime))
    } else {
      None
    }
}

object HRavenReducerHistoryService extends HRavenReducerHistoryService

class HRavenRatioBasedEstimator extends RatioBasedEstimator {
  override val historyService = HRavenReducerHistoryService
}

class HRavenRuntimeBasedEstimator extends RuntimeReducerEstimator {
  override val historyService = HRavenReducerHistoryService
}
