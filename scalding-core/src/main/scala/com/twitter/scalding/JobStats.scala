/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import java.io.{ File, OutputStream }
import scala.collection.JavaConverters._
import cascading.flow.Flow
import cascading.stats.{CascadeStats, CascadingStats, FlowStats}

import scala.util.Try

object JobStats {
  def apply(stats: CascadingStats): JobStats = {
    val m = statsMap(stats)
    new JobStats(
      stats match {
        case cs: CascadeStats => m
        case fs: FlowStats => m + ("flow_step_stats" -> fs.getFlowStepStats.asScala.map(statsMap))
      }
    )
  }

  private def counterMap(stats: CascadingStats): Map[String, Any] =
    stats.getCounterGroups.asScala.map { group =>
      (group, stats.getCountersFor(group).asScala.map { counter =>
        (counter, stats.getCounterValue(group, counter))
      }.toMap)
    }.toMap

  private def statsMap(stats: CascadingStats): Map[String, Any] =
    Map(
      "counters" -> counterMap(stats),
      "duration" -> stats.getDuration,
      "finished_time" -> stats.getFinishedTime,
      "id" -> stats.getID,
      "name" -> stats.getName,
      "run_time" -> stats.getRunTime,
      "start_time" -> stats.getStartTime,
      "submit_time" -> stats.getSubmitTime,
      "failed" -> stats.isFailed,
      "skipped" -> stats.isSkipped,
      "stopped" -> stats.isStopped,
      "successful" -> stats.isSuccessful
    )

  def toJsonValue(a: Any): String = {
    Try(a.toString.toInt)
      .recoverWith { case t: Throwable => Try(a.toString.toDouble) }
      .recover { case t: Throwable =>
          val s = a.toString
          "\"%s\"".format(s)
      }
      .get
      .toString
  }
}

// Simple wrapper for a Map that contains the useful info from the job flow's stats
// If you want to write this, call toMap and use json, etc... to write it
case class JobStats(toMap: Map[String, Any]) {
  def toJson: String =
    toMap.map { case (k, v) => "\"%s\" : %s".format(k, JobStats.toJsonValue(v))}
      .mkString("{",",","}")
}
