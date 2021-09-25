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

import java.util.TimeZone

import org.apache.hadoop.conf.Configuration

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

  /**
   * Gives all paths in the given daterange with windows based on the provided duration.
   */
  def allPathsWithDuration(pattern: String, duration: Duration, dateRange: DateRange, tz: TimeZone): Iterable[String] =
    // This method is exhaustive, but too expensive for Cascading's JobConf writing.
    dateRange.each(duration)
      .map { dr: DateRange =>
        toPath(pattern, dr.start, tz)
      }

  /**
   * Gives all read paths in the given daterange.
   */
  def readPathsFor(pattern: String, dateRange: DateRange, tz: TimeZone): Iterable[String] = {
    TimePathedSource.stepSize(pattern, tz) match {
      case Some(duration) => allPathsWithDuration(pattern, duration, dateRange, tz)
      case None => sys.error(s"No suitable step size for pattern: $pattern")
    }
  }

  /**
   * Gives the write path based on daterange end.
   */
  def writePathFor(pattern: String, dateRange: DateRange, tz: TimeZone): String = {
    assert(pattern != "/*", "Pattern must not be /*")
    assert(pattern.takeRight(2) == "/*", "Pattern must end with /* " + pattern)
    val stripped = pattern.dropRight(2)
    toPath(stripped, dateRange.end, tz)
  }
}

abstract class TimeSeqPathedSource(val patterns: Seq[String], val dateRange: DateRange, val tz: TimeZone) extends FileSource {

  override def hdfsPaths = patterns
    .flatMap{ pattern: String =>
      Globifier(pattern)(tz).globify(dateRange)
    }

  /**
   * Override this if you have for instance an hourly pattern but want to run every 6 hours.
   * By default, we call TimePathedSource.stepSize(pattern, tz)
   */
  protected def defaultDurationFor(pattern: String): Option[Duration] =
    TimePathedSource.stepSize(pattern, tz)

  protected def allPathsFor(pattern: String): Iterable[String] =
    defaultDurationFor(pattern) match {
      case Some(duration) => TimePathedSource.allPathsWithDuration(pattern, duration, dateRange, tz)
      case None => sys.error(s"No suitable step size for pattern: $pattern")
    }

  /** These are all the paths we will read for this data completely enumerated */
  def allPaths: Iterable[String] =
    patterns.flatMap(allPathsFor(_))

  /**
   * Get path statuses based on daterange. This tests each path with pathIsGood
   * (which by default checks that there is at least on file in that directory)
   */
  def getPathStatuses(conf: Configuration): Iterable[(String, Boolean)] =
    allPaths.map { path => (path, pathIsGood(path, conf)) }

  // Override because we want to check UNGLOBIFIED paths that each are present.
  override def hdfsReadPathsAreGood(conf: Configuration): Boolean =
    getPathStatuses(conf).forall {
      case (path, good) =>
        if (!good) {
          System.err.println("[ERROR] Path: " + path + " is missing in: " + toString)
        }
        good
    }

  override def toString = "TimeSeqPathedSource(" + patterns.mkString(",") +
    ", " + dateRange + ", " + tz + ")"

  override def equals(that: Any) =
    (that != null) &&
      (this.getClass == that.getClass) &&
      this.patterns == that.asInstanceOf[TimeSeqPathedSource].patterns &&
      this.dateRange == that.asInstanceOf[TimeSeqPathedSource].dateRange &&
      this.tz == that.asInstanceOf[TimeSeqPathedSource].tz

  override def hashCode = patterns.hashCode +
    31 * dateRange.hashCode +
    (31 ^ 2) * tz.hashCode
}

/**
 * This will automatically produce a globbed version of the given path.
 * THIS MEANS YOU MUST END WITH A / followed by * to match a file
 * For writing, we write to the directory specified by the END time.
 */
abstract class TimePathedSource(val pattern: String,
  dateRange: DateRange,
  tz: TimeZone) extends TimeSeqPathedSource(Seq(pattern), dateRange, tz) {

  //Write to the path defined by the end time:
  override def hdfsWritePath = TimePathedSource.writePathFor(pattern, dateRange, tz)

  override def localPaths = patterns
    .flatMap { pattern: String =>
      Globifier(pattern)(tz).globify(dateRange)
    }
}

/*
 * A source that contains the most recent existing path in this date range.
 */
abstract class MostRecentGoodSource(p: String, dr: DateRange, t: TimeZone)
  extends TimePathedSource(p, dr, t) {

  override def toString =
    "MostRecentGoodSource(" + p + ", " + dr + ", " + t + ")"

  override protected def goodHdfsPaths(hdfsMode: Hdfs) = getPathStatuses(hdfsMode.jobConf)
    .toList
    .reverse
    .find(_._2)
    .map(_._1)

  override def hdfsReadPathsAreGood(conf: Configuration) = getPathStatuses(conf)
    .exists(_._2)
}
