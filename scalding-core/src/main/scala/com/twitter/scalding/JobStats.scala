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

import scala.collection.JavaConverters._
import cascading.stats.{ CascadeStats, CascadingStats, FlowStats }

import scala.util.{ Failure, Try }

object JobStats {
  def apply(stats: CascadingStats[_]): JobStats = {
    val m = statsMap(stats)
    new JobStats(
      stats match {
        case cs: CascadeStats => m
        case fs: FlowStats => m + ("flow_step_stats" -> fs.getFlowStepStats.asScala.map(statsMap))
      })
  }

  private def counterMap(stats: CascadingStats[_]): Map[String, Map[String, Long]] =
    stats.getCounterGroups.asScala.map { group =>
      (group, stats.getCountersFor(group).asScala.map { counter =>
        (counter, stats.getCounterValue(group, counter))
      }.toMap)
    }.toMap

  private def statsMap(stats: CascadingStats[_]): Map[String, Any] =
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
      "successful" -> stats.isSuccessful)

  /**
   * Returns the counters with Group String -> Counter String -> Long
   */
  def toCounters(cMap: Any): Try[Map[String, Map[String, Long]]] =
    // This really sucks, but this is what happens when you let Map[String, Any] into your code
    cMap match {
      case m: Map[_, _] => Try {
        m.foldLeft(Map.empty[String, Map[String, Long]]) {
          case (acc, (k: String, v: Any)) => v match {
            case m: Map[_, _] =>
              acc + (k -> m.foldLeft(Map.empty[String, Long]) {
                case (acc2, (k: String, v: Long)) => acc2 + (k -> v)
                case (_, kv) => sys.error("inner k, v not (String, Long):" + kv)
              })
            case _ => sys.error("inner values are not Maps: " + v)
          }
          case kv => sys.error("Map does not contain string keys: " + (kv))
        }
      }
      case _ => Failure(new Exception("%s not a Map[String, Any]".format(cMap)))
    }

  def toJsonValue(a: Any): String =
    if (a == null) "null"
    else {
      Try(a.toString.toInt)
        .recoverWith { case t: Throwable => Try(a.toString.toDouble) }
        .recover {
          case t: Throwable =>
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
  def counters: Map[String, Map[String, Long]] =
    toMap.get("counters")
      .map(JobStats.toCounters(_))
      .getOrElse(sys.error("counters missing from: " + toMap))
      .get

  def toJson: String =
    toMap.map { case (k, v) => "\"%s\" : %s".format(k, JobStats.toJsonValue(v)) }
      .mkString("{", ",", "}")
}
