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

// import cascading.tap.MultiSourceTap
// import cascading.tap.SinkMode
// import cascading.tap.Tap
// import cascading.tap.local.FileTap
// import cascading.tuple.{Tuple, TupleEntry, TupleEntryIterator, Fields}

import com.etsy.cascading.tap.local.LocalTap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

object TimePathedSource {
  val YEAR_MONTH_DAY = "/%1$tY/%1$tm/%1$td"
  val YEAR_MONTH_DAY_HOUR = "/%1$tY/%1$tm/%1$td/%1$tH"
}

abstract class TimeSeqPathedSource(val patterns : Seq[String], val dateRange : DateRange, val tz : TimeZone) extends FileSource {
  override def hdfsPaths = patterns
    .flatMap{ pattern: String =>
      Globifier(pattern)(tz).globify(dateRange)
    }  

  /*
   * Get path statuses based on daterange.
   */
  protected def getPathStatuses(pattern: String, conf : Configuration) : Iterable[(String, Boolean)] = {
    List("%1$tH" -> Hours(1), "%1$td" -> Days(1)(tz),
      "%1$tm" -> Months(1)(tz), "%1$tY" -> Years(1)(tz))
      .find { unitDur : (String,Duration) => pattern.contains(unitDur._1) }
      .map { unitDur =>
        // This method is exhaustive, but too expensive for Cascading's JobConf writing.
        dateRange.each(unitDur._2)
          .map { dr : DateRange =>
          val path = String.format(pattern, dr.start.toCalendar(tz))
          val good = pathIsGood(path, conf)
          (path, good)
        }
      }
      .getOrElse(Nil : Iterable[(String, Boolean)])
  }

  // Override because we want to check UNGLOBIFIED paths that each are present.
  override def hdfsReadPathsAreGood(conf : Configuration) : Boolean = {
    patterns.forall{ pattern =>
      getPathStatuses(pattern, conf).forall{ x =>
        if (!x._2) {
          System.err.println("[ERROR] Path: " + x._1 + " is missing in: " + toString)
        }
        x._2
      }
    }
  }

  override def toString = "TimeSeqPathedSource(" + patterns.reduce(_ + "," + _) +
      ", " + dateRange + ", " + tz + ")"

  override def equals(that : Any) =
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
abstract class TimePathedSource(val pattern : String, override val dateRange : DateRange, override val tz : TimeZone)
    extends TimeSeqPathedSource(Seq(pattern), dateRange, tz) {
  //Write to the path defined by the end time:
  override def hdfsWritePath = {
    // TODO this should be required everywhere but works on read without it
    // maybe in 0.9.0 be more strict
    assert(pattern.takeRight(2) == "/*", "Pattern must end with /* " + pattern)
    val lastSlashPos = pattern.lastIndexOf('/')
    val stripped = pattern.slice(0,lastSlashPos)
    String.format(stripped, dateRange.end.toCalendar(tz))
  }
  override def localPath = pattern

  override def hashCode = patterns.hashCode +
    31 * dateRange.hashCode +
    (31 ^ 2) * tz.hashCode
}

/*
 * A source that contains the most recent existing path in this date range.
 */
abstract class MostRecentGoodSource(p : String, dr : DateRange, t : TimeZone)
    extends TimePathedSource(p, dr, t) {

  override def toString =
    "MostRecentGoodSource(" + p + ", " + dr + ", " + t + ")"

  override protected def goodHdfsPaths(hdfsMode : Hdfs) = getPathStatuses(p, hdfsMode.jobConf)
    .toList
    .reverse
    .find{ _._2 }
    .map{ x => x._1 }

  override def hdfsReadPathsAreGood(conf : Configuration) = getPathStatuses(p, conf)
    .exists{ _._2 }
}


