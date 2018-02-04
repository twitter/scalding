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

import java.util.Calendar
import java.util.TimeZone

import scala.annotation.tailrec

/**
 * Represents millisecond based duration (non-calendar based): seconds, minutes, hours
 * calField should be a java.util.Calendar field
 */
object Duration extends java.io.Serializable {
  // TODO: remove this in 0.9.0
  val SEC_IN_MS: Int = 1000
  val MIN_IN_MS: Int = 60 * SEC_IN_MS
  val HOUR_IN_MS: Int = 60 * MIN_IN_MS
  val UTC_UNITS: List[(Int => AbsoluteDuration, Int)] =
    List[(Int => AbsoluteDuration, Int)]((Hours, HOUR_IN_MS), (Minutes, MIN_IN_MS), (Seconds, SEC_IN_MS), (Millisecs, 1))
}

abstract class Duration(val calField: Int, val count: Int, val tz: TimeZone)
  extends java.io.Serializable {
  protected def calAdd(that: RichDate, steps: Int) = {
    val cal = that.toCalendar(tz)
    cal.setLenient(true)
    cal.add(calField, steps)
    RichDate(cal)
  }

  def addTo(that: RichDate) = calAdd(that, count)

  def subtractFrom(that: RichDate) = calAdd(that, -count)

  // Return the latest RichDate at boundary of this time unit, ignoring
  // the count of the units.  Like a truncation.
  // Only makes sense for non-mixed durations.
  def floorOf(that: RichDate): RichDate = {
    val cal = that.toCalendar(tz)
    RichDate(CalendarOps.truncate(cal, calField))
  }
}

case class Days(cnt: Int)(implicit tz: TimeZone)
  extends Duration(Calendar.DAY_OF_MONTH, cnt, tz)

case class Weeks(cnt: Int)(implicit tz: TimeZone)
  extends Duration(Calendar.WEEK_OF_YEAR, cnt, tz) {

  // The library we are using can't handle week truncation...
  override def floorOf(that: RichDate) = {
    val step = Days(1)
    @tailrec def recentMonday(rd: RichDate): RichDate = {
      rd.toCalendar(tz).get(Calendar.DAY_OF_WEEK) match {
        case Calendar.MONDAY => rd
        case _ => recentMonday(step.subtractFrom(rd))
      }
    }
    //Set it to the earliest point in the day:
    step.floorOf(recentMonday(that))
  }
}

case class Months(cnt: Int)(implicit tz: TimeZone)
  extends Duration(Calendar.MONTH, cnt, tz)

case class Years(cnt: Int)(implicit tz: TimeZone)
  extends Duration(Calendar.YEAR, cnt, tz)

abstract class AbstractDurationList[T <: Duration](parts: List[T]) extends Duration(-1, -1, null) {
  override def addTo(that: RichDate) = {
    parts.foldLeft(that) { (curdate, next) => next.addTo(curdate) }
  }
  override def subtractFrom(that: RichDate) = {
    parts.foldLeft(that) { (curdate, next) => next.subtractFrom(curdate) }
  }
  //This does not make sense for a DurationList interval, pass through
  override def floorOf(that: RichDate) = that
}

case class DurationList(parts: List[Duration]) extends AbstractDurationList[Duration](parts)
