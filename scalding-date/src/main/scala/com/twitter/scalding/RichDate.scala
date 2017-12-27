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

import java.text.SimpleDateFormat

import java.util.Calendar
import java.util.Date
import java.util.TimeZone

/**
 * RichDate adds some nice convenience functions to the Java date/calendar classes
 * We commonly do Date/Time work in analysis jobs, so having these operations convenient
 * is very helpful.
 */
object RichDate {
  // Implicits to Java types:
  implicit def toDate(rd: RichDate): Date = rd.value
  implicit def toCalendar(rd: RichDate)(implicit tz: TimeZone): Calendar = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(rd.value)
    cal
  }

  implicit def apply(d: Date): RichDate = RichDate(d.getTime)
  implicit def apply(d: Calendar): RichDate = RichDate(d.getTime)
  /**
   * Parse the string with one of the value DATE_FORMAT_VALIDATORS in the order listed in DateOps.
   * We allow either date, date with time in minutes, date with time down to seconds.
   * The separator between date and time can be a space or "T".
   */
  implicit def apply(str: String)(implicit tz: TimeZone, dp: DateParser): RichDate =
    dp.parse(str).get

  /* If the format is one of the truncated DateOps formats, we can do
   * the upper bound, else go to the end of the day
   */
  def upperBound(s: String)(implicit tz: TimeZone, dp: DateParser) = {
    val end = apply(s)
    (DateOps.getFormatObject(s) match {
      case Some(DateOps.Format.DATE_WITHOUT_DASH) => end + Days(1)
      case Some(DateOps.Format.DATE_WITH_DASH) => end + Days(1)
      case Some(DateOps.Format.DATEHOUR_WITHOUT_DASH) => end + Hours(1)
      case Some(DateOps.Format.DATEHOUR_WITH_DASH) => end + Hours(1)
      case Some(DateOps.Format.DATETIME_WITHOUT_DASH) => end + Minutes(1)
      case Some(DateOps.Format.DATETIME_WITH_DASH) => end + Minutes(1)
      case Some(DateOps.Format.DATETIME_HMS_WITHOUT_DASH) => end + Seconds(1)
      case Some(DateOps.Format.DATETIME_HMS_WITH_DASH) => end + Seconds(1)
      case Some(DateOps.Format.DATETIME_HMSM_WITH_DASH) => end + Millisecs(2)
      case None => Days(1).floorOf(end + Days(1))
    }) - Millisecs(1)
  }

  def now: RichDate = RichDate(System.currentTimeMillis())

  implicit def richDateOrdering: Ordering[RichDate] = new Ordering[RichDate] {
    def compare(a: RichDate, b: RichDate) = java.lang.Long.compare(a.timestamp, b.timestamp)
  }
}

/**
 * A value class wrapper for milliseconds since the epoch. Its tempting to extend
 * this with AnyVal but this causes problem with Java code.
 */
case class RichDate(val timestamp: Long) extends Ordered[RichDate] {
  // these are mutable, don't keep them around
  def value: Date = new java.util.Date(timestamp)

  def +(interval: Duration) = interval.addTo(this)
  def -(interval: Duration) = interval.subtractFrom(this)

  //Inverse of the above, d2 + (d1 - d2) == d1
  def -(that: RichDate) = AbsoluteDuration.fromMillisecs(timestamp - that.timestamp)

  override def compare(that: RichDate): Int =
    java.lang.Long.compare(timestamp, that.timestamp)

  //True of the other is a RichDate with equal value, or a Date equal to value
  override def equals(that: Any) =
    that match {
      case d: Date => d.getTime == timestamp
      case RichDate(ts) => ts == timestamp
      case _ => false
    }

  def before(that: RichDate): Boolean = compare(that) < 0
  def after(that: RichDate): Boolean = compare(that) > 0

  /**
   * Use String.format to format the date, as opposed to toString, which uses SimpleDateFormat.
   */
  def format(pattern: String)(implicit tz: TimeZone): String = String.format(pattern, toCalendar(tz))

  /**
   * Make sure the hashCode is the same as Date for the (questionable) choice
   * to make them equal. this is the same as what java does (and only sane thing):
   * http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/6-b14/java/util/Date.java#989
   */
  override def hashCode =
    (timestamp.toInt) ^ ((timestamp >> 32).toInt)

  def toCalendar(implicit tz: TimeZone) = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(value)
    cal
  }
  override def toString = value.toString

  /**
   * Use SimpleDateFormat to print the string
   */
  def toString(fmt: String)(implicit tz: TimeZone): String = {
    val cal = toCalendar(tz)
    val sdfmt = new SimpleDateFormat(fmt)
    sdfmt.setCalendar(cal)
    sdfmt.format(cal.getTime)
  }
}

