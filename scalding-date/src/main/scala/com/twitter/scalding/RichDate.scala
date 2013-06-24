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

import com.joestelmach.natty

/**
* RichDate adds some nice convenience functions to the Java date/calendar classes
* We commonly do Date/Time work in analysis jobs, so having these operations convenient
* is very helpful.
*/
object RichDate {
  def upperBound(s : String)(implicit tz : TimeZone): Date = {
    val end = apply(s)(tz)
    (DateOps.getFormat(s) match {
      case Some(DateOps.DATE_WITH_DASH) => end + Days(1)
      case Some(DateOps.DATEHOUR_WITH_DASH) => end + Hours(1)
      case Some(DateOps.DATETIME_WITH_DASH) => end + Minutes(1)
      case Some(DateOps.DATETIME_HMS_WITH_DASH) => end + Seconds(1)
      case Some(DateOps.DATETIME_HMSM_WITH_DASH) => end + Millisecs(2)
      case None => Days(1).floorOf(end + Days(1))
    }) - Millisecs(1)
  }

  /**
  * Parse the string with one of the value DATE_FORMAT_VALIDATORS in the order listed above.
  * We allow either date, date with time in minutes, date with time down to seconds.
  * The separator between date and time can be a space or "T".
  */
  def apply(str : String)(implicit tz : TimeZone): RichDate = {
   val newStr = str
                  .replace("T"," ") //We allow T to separate dates and times, just remove it and then validate
                  .replaceAll("[/_]", "-")  // Allow for slashes and underscores
    DateOps.getFormat(newStr) match {
      case Some(fmtStr) =>
        val cal = Calendar.getInstance(tz)
        val formatter = new SimpleDateFormat(fmtStr)
        formatter.setCalendar(cal)
        new RichDate(formatter.parse(newStr))
      case None => // try to parse with Natty
        val timeParser = new natty.Parser(tz)
        val dateGroups = timeParser.parse(str)
        if (dateGroups.size == 0) {
          throw new IllegalArgumentException("Could not convert string: '" + str + "' into a date.")
        }
        // a DateGroup can have more than one Date (e.g. if you do "Sept. 11th or 12th"),
        // but we're just going to take the first
        val dates = dateGroups.get(0).getDates()
        new RichDate(dates.get(0))
    }
  }
  // Really isn't safe, be careful
  implicit def fromString(s: String)(implicit tz: TimeZone): RichDate = apply(s)

  def apply(ts : Long): RichDate = new RichDate(new Date(ts))
  implicit def fromTimestamp(ts : Long): RichDate = apply(ts)

  def apply(d : Date): RichDate  = new RichDate(d)
  // allow individual imports
  implicit def fromDate(d : Date): RichDate = apply(d)

  implicit def toDate(rd : RichDate): Date = rd.value

  implicit def fromCalendar(cal: Calendar): RichDate = apply(cal.getTime())

  def now: RichDate = apply(System.currentTimeMillis)
}

/** This is an enrichment of the java.util.Date class which you should not
 * use in your code.
 * TODO make this extend AnyVal in scala 2.10
 */
class RichDate(val value : Date) extends Ordered[RichDate] {
  def +(interval : Duration): Date = interval.addTo(this)
  def -(interval : Duration): Date = interval.subtractFrom(this)

  //Inverse of the above, d2 + (d1 - d2) == d1
  def -(that : Date): AbsoluteDuration = AbsoluteDuration.fromMillisecs(value.getTime - that.getTime)

  override def compare(that : RichDate) : Int = {
    if (value.before(that.value)) {
      -1
    }
    else if (value.after(that.value)) {
      1
    } else {
      0
    }
  }

  //True of the other is a RichDate with equal value, or a Date equal to value
  override def equals(that : Any) = that match {
    case d: Date => d == value
    case rd: RichDate => rd.value == value
    case _ => false
  }

  /** Use String.format to format the date, as opposed to toString with uses SimpleDateFormat
   */
  def format(pattern: String)(implicit tz: TimeZone) : String = String.format(pattern, toCalendar(tz))

  override def hashCode = { value.hashCode }

  //milliseconds since the epoch
  def timestamp : Long = value.getTime

  def toCalendar(implicit tz: TimeZone): Calendar = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(value)
    cal
  }
  override def toString = {
    value.toString
  }

  /** Use SimpleDateFormat to print the string
   */
  def toString(fmt : String)(implicit tz : TimeZone) : String = {
    val cal = toCalendar(tz)
    val sdfmt = new SimpleDateFormat(fmt)
    sdfmt.setCalendar(cal)
    sdfmt.format(cal.getTime)
  }
}

