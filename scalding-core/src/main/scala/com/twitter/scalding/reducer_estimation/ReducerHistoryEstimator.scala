package com.twitter.scalding.reducer_estimation

import com.twitter.scalding.estimation.{ HistoryEstimator, Task }
import org.apache.hadoop.mapred.JobConf

object ReducerHistoryEstimator {
  val Status = "status"
  val StartTime = "startTime"
  val FinishTime = "finishTime"

  implicit class ReducerRichTask(val task: Task) {
    def status: Option[String] = task.details.get(Status).map(_.asInstanceOf[String])
    def startTime: Option[Long] = task.details.get(StartTime).map(_.asInstanceOf[Long])
    def finishTime: Option[Long] = task.details.get(FinishTime).map(_.asInstanceOf[Long])
  }
}

trait ReducerHistoryEstimator extends HistoryEstimator[Int] {
  override def maxHistoryItems(conf: JobConf): Int = ReducerEstimatorConfig.getMaxHistory(conf)
}
