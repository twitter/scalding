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
package com.twitter.scalding.source

import java.util.TimeZone

import org.slf4j.{ Logger, LoggerFactory => LogManager }

import com.twitter.scalding.{ RichDate, DateRange, Hours, Days, Duration, Months, Years, Globifier }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

object TimePathedSource {
  val YEAR_MONTH_DAY = "/%1$tY/%1$tm/%1$td"
  val YEAR_MONTH_DAY_HOUR = "/%1$tY/%1$tm/%1$td/%1$tH"

  def toPath(pattern: String, date: RichDate, tz: TimeZone): String =
    String.format(pattern, date.toCalendar(tz))

  def stepSize(pattern: String, tz: TimeZone): Option[Duration] =
    List("%1$tH" -> Hours(1), "%1$td" -> Days(1)(tz),
      "%1$tm" -> Months(1)(tz), "%1$tY" -> Years(1)(tz))
      .find { unitDur: (String, Duration) => pattern.contains(unitDur._1) }
      .map(_._2)
}

abstract class TimeSeqPathedSource[T, M <: RDD[T]](val patterns: Seq[String], val dateRange: DateRange, val tz: TimeZone) extends FileSource[T, M] {
  @transient private val logger: Logger = LogManager.getLogger(this.getClass)

  override def hdfsPaths = patterns
    .flatMap{ pattern: String =>
      Globifier(pattern)(tz).globify(dateRange)
    }

  protected def allPathsFor(pattern: String): Iterable[String] =
    TimePathedSource.stepSize(pattern, tz)
      .map { dur =>
        // This method is exhaustive, but too expensive for Cascading's JobConf writing.
        dateRange.each(dur)
          .map { dr: DateRange =>
            TimePathedSource.toPath(pattern, dr.start, tz)
          }
      }
      .getOrElse(Nil)

  /** These are all the paths we will read for this data completely enumerated */
  def allPaths: Iterable[String] =
    patterns.flatMap(allPathsFor(_))

  /**
   * Get path statuses based on daterange. This tests each path with pathIsGood
   * (which by default checks that there is at least on file in that directory)
   */
  def getPathStatuses(job: Job): Iterable[(String, Boolean)] =
    allPaths.map { path => (path, pathIsGood(path, job)) }

  // Override because we want to check UNGLOBIFIED paths that each are present.
  override def hdfsReadPathsAreGood(job: Job): Boolean =
    getPathStatuses(job).forall {
      case (path, good) =>
        if (!good) {
          logger.error("[ERROR] Path: " + path + " is missing in: " + toString)
        }
        good
    }

  override def toString = "TimeSeqPathedSource(" + patterns.mkString(",") +
    ", " + dateRange + ", " + tz + ")"

  override def equals(that: Any) =
    (that != null) &&
      (this.getClass == that.getClass) &&
      this.patterns == that.asInstanceOf[TimeSeqPathedSource[T, M]].patterns &&
      this.dateRange == that.asInstanceOf[TimeSeqPathedSource[T, M]].dateRange &&
      this.tz == that.asInstanceOf[TimeSeqPathedSource[T, M]].tz

  override def hashCode = patterns.hashCode +
    31 * dateRange.hashCode +
    (31 ^ 2) * tz.hashCode
}

/**
 * This will automatically produce a globbed version of the given path.
 * THIS MEANS YOU MUST END WITH A / followed by * to match a file
 * For writing, we write to the directory specified by the END time.
 */
abstract class TimePathedSource[T, M <: RDD[T]](val pattern: String,
  dateRange: DateRange,
  tz: TimeZone) extends TimeSeqPathedSource[T, M](Seq(pattern), dateRange, tz) {

  //Write to the path defined by the end time:
  override def hdfsWritePath = {
    // TODO this should be required everywhere but works on read without it
    // maybe in 0.9.0 be more strict
    assert(pattern.takeRight(2) == "/*", "Pattern must end with /* " + pattern)
    val lastSlashPos = pattern.lastIndexOf('/')
    val stripped = pattern.slice(0, lastSlashPos)
    TimePathedSource.toPath(stripped, dateRange.end, tz)
  }
  def localPath = pattern
}

/*
 * A source that contains the most recent existing path in this date range.
 */
abstract class MostRecentGoodSource[T, M <: RDD[T]](p: String, dr: DateRange, t: TimeZone)
  extends TimePathedSource[T, M](p, dr, t) {

  override def toString =
    "MostRecentGoodSource(" + p + ", " + dr + ", " + t + ")"

  override def goodPaths(job: Job) = getPathStatuses(job)
    .toList
    .reverse
    .find(_._2)
    .map(_._1)

  override def hdfsReadPathsAreGood(job: Job) = getPathStatuses(job)
    .exists(_._2)
}
