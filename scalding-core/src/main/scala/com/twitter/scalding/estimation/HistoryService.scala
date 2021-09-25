package com.twitter.scalding.estimation

import scala.util.Try

/**
 * Info about a prior FlowStep, provided by implementers of HistoryService
 */
final case class FlowStepHistory(
  keys: FlowStepKeys,
  submitTimeMillis: Long,
  launchTimeMillis: Long,
  finishTimeMillis: Long,
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

final case class FlowStepKeys(
  jobName: String,
  user: String,
  priority: String,
  status: String,
  version: String,
  queue: String)

final case class Task(details: Map[String, Any], counters: Map[String, Long]) {
  def taskType: Option[String] = details.get(Task.TaskType).map(_.asInstanceOf[String])
}

object Task {
  val TaskType = "taskType"
}

trait HistoryService {
  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]]
}

