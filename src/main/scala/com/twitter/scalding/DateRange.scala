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

import scala.annotation.tailrec
import scala.util.matching.Regex

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.util.NoSuchElementException

import org.apache.commons.lang.time.DateUtils

/**
* Holds some coversion functions for dealing with strings as RichDate objects
*/
@serializable
object DateOps {
  val PACIFIC = TimeZone.getTimeZone("America/Los_Angeles")
  val UTC = TimeZone.getTimeZone("UTC")

  val DATE_WITH_DASH = "yyyy-MM-dd"
  val DATEHOUR_WITH_DASH = "yyyy-MM-dd HH"
  val DATETIME_WITH_DASH = "yyyy-MM-dd HH:mm"
  val DATETIME_HMS_WITH_DASH = "yyyy-MM-dd HH:mm:ss"
  val DATETIME_HMSM_WITH_DASH = "yyyy-MM-dd HH:mm:ss.SSS"

  private val DATE_RE = """\d{4}-\d{2}-\d{2}"""
  private val SEP_RE = """(T?|\s*)"""
  private val DATE_FORMAT_VALIDATORS = List(DATE_WITH_DASH -> new Regex("""^\s*""" + DATE_RE + """\s*$"""),
                                            DATEHOUR_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d\s*$"""),
                                            DATETIME_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d:\d\d\s*$"""),
                                            DATETIME_HMS_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d:\d\d:\d\d\s*$"""),
                                            DATETIME_HMSM_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d:\d\d:\d\d\.\d{1,3}\s*$"""))
  /**
  * Return the guessed format for this datestring
  */
  def getFormat(s : String) = DATE_FORMAT_VALIDATORS.find{_._2.findFirstIn(s).isDefined}.get._1

  /**
  * Parse the string with one of the value DATE_FORMAT_VALIDATORS in the order listed above.
  * We allow either date, date with time in minutes, date with time down to seconds.
  * The separator between date and time can be a space or "T".
  */
  implicit def stringToRichDate(str : String)(implicit tz : TimeZone) = {
    try {
      //We allow T to separate dates and times, just remove it and then validate:
      val newStr = str.replace("T"," ")
      val fmtStr = getFormat(newStr)
      val cal = Calendar.getInstance(tz)
      val formatter = new SimpleDateFormat(fmtStr)
      formatter.setCalendar(cal)
      new RichDate(formatter.parse(newStr))
    } catch {
      case e: NoSuchElementException => throw new IllegalArgumentException("Could not convert string: '" + str + "' into a date.")
    }
  }
  implicit def longToRichDate(ts : Long) = new RichDate(new Date(ts))
  implicit def dateToRichDate(d : Date) = new RichDate(d)
  implicit def richDateToDate(rd : RichDate) = rd.value
  implicit def richDateToCalendar(rd : RichDate)(implicit tz : TimeZone) = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(rd.value)
    cal
  }
  implicit def calendarToRichDate(cal : Calendar) = RichDate(cal.getTime())
}

/**
* Represents millisecond based duration (non-calendar based): seconds, minutes, hours
*/
abstract class Duration {
  def toMillisecs : Long
  def toSeconds = toMillisecs / 1000.0
  def +(that : Duration) = Millisecs(toMillisecs + that.toMillisecs)
  def -(that : Duration) = Millisecs(toMillisecs - that.toMillisecs)
  def *(that : Double) = Millisecs((toMillisecs * that).toLong)
  def /(that : Double) = Millisecs((toMillisecs / that).toLong)
}
case class Millisecs(value : Long) extends Duration {
  def toMillisecs = value
}
case class Seconds(value : Long) extends Duration {
  def toMillisecs = value * 1000L
}
case class Minutes(value : Long) extends Duration {
  def toMillisecs = value * 60 * 1000L
}
case class Hours(value : Long) extends Duration {
  def toMillisecs = value * 60 * 60 * 1000L
}

/**
* this is only relative to a calendar
*/
abstract class CalendarDuration {
  def toDays : Int
  def +(that : CalendarDuration) = Days(toDays + that.toDays)
  def -(that : CalendarDuration) = Days(toDays - that.toDays)
}

case class Days(value : Int) extends CalendarDuration {
  def toDays = value
}
case class Weeks(value : Int) extends CalendarDuration {
  def toDays = value * 7
}

/**
* RichDate adds some nice convenience functions to the Java date/calendar classes
* We commonly do Date/Time work in analysis jobs, so having these operations convenient
* is very helpful.
*/
object RichDate {
  def apply(s : String)(implicit tz : TimeZone) = {
    DateOps.stringToRichDate(s)(tz)
  }
  def apply(l : Long) = {
    DateOps.longToRichDate(l)
  }
  def upperBound(s : String)(implicit tz : TimeZone) = {
    val end = apply(s)(tz)
    (DateOps.getFormat(s) match {
      case DateOps.DATE_WITH_DASH => end + Days(1)
      case DateOps.DATEHOUR_WITH_DASH => end + Hours(1)
      case DateOps.DATETIME_WITH_DASH => end + Minutes(1)
      case DateOps.DATETIME_HMS_WITH_DASH => end + Seconds(1)
      case DateOps.DATETIME_HMSM_WITH_DASH => end + Millisecs(2)
    }) - Millisecs(1)
  }
}

case class RichDate(val value : Date) extends Ordered[RichDate] {
  def +(interval : Duration) = new RichDate(new Date(value.getTime + interval.toMillisecs))
  def -(interval : Duration) = new RichDate(new Date(value.getTime - interval.toMillisecs))

  def +(interval : CalendarDuration)(implicit tz : TimeZone) = {
    val cal = toCalendar(tz)
    cal.setLenient(true)
    cal.add(Calendar.DAY_OF_YEAR, interval.toDays)
    new RichDate(cal.getTime)
  }
  def -(interval : CalendarDuration)(implicit tz : TimeZone) = {
    val cal = toCalendar(tz)
    cal.setLenient(true)
    cal.add(Calendar.DAY_OF_YEAR, -(interval.toDays))
    new RichDate(cal.getTime)
  }

  //Inverse of the above, d2 + (d1 - d2) == d1
  def -(that : RichDate) : Duration = {
    val diff = value.getTime - that.value.getTime
    val units = List(Hours,Minutes,Seconds,Millisecs)
    //We can't fail the last one, x % 1 == 0
    val d_unit = units.find { u : Function1[Long,Duration] =>
      (diff % u(1).toMillisecs) == 0
    }.head
    d_unit( diff / d_unit(1).toMillisecs )
  }
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

  private def earliestIn(calField : Int, tz : TimeZone) : RichDate = {
    val cal = toCalendar(tz)
    new RichDate(DateUtils.truncate(cal, calField).getTime)
  }
  /**
  * Truncate to the earliest millisecond in the same hour as this time, in the given TZ.
  */
  def earliestInHour(implicit tz : TimeZone) = earliestIn(Calendar.HOUR, tz)
  /**
  * Truncate to the earliest millisecond in the same day as this time, in the given TZ.
  */
  def earliestInDay(implicit tz : TimeZone) = earliestIn(Calendar.DAY_OF_MONTH, tz)
  /**
  * Truncate to the earliest millisecond in the most recent Monday as this time, in the given TZ.
  */
  def earliestInWeek(implicit tz : TimeZone) = {
    @tailrec def recentMonday(cal : Calendar) : Calendar = {
      cal.get(Calendar.DAY_OF_WEEK) match {
        case Calendar.MONDAY => cal
        case _ => {
          //The sorrows of the highly mutable Java standard library
          val newc = cal.clone().asInstanceOf[Calendar];
          //Make it clear we want to interpret a previous day at the beginning
          //of the year/week as the previous week
          newc.setLenient(true)
          newc.add(Calendar.DAY_OF_MONTH, -1)
          recentMonday(newc)
        }
      }
    }
    val mon = recentMonday(toCalendar(tz))
    //Set it to the earliest point in the day:
    DateUtils.truncate(mon, Calendar.DATE)
    new RichDate(mon.getTime)
  }

  //True of the other is a RichDate with equal value, or a Date equal to value
  override def equals(that : Any) = {
    //Due to type erasure (scala 2.9 complains), we need to use a manifest:
    def opInst[T : Manifest](v : Any) = {
      val klass = manifest[T].erasure
      if(null != v && klass.isInstance(v)) Some(v.asInstanceOf[T]) else None
    }
    opInst[RichDate](that)
      .map( _.value)
      .orElse(opInst[Date](that))
      .map( _.equals(value) )
      .getOrElse(false)
  }
  override def hashCode = { value.hashCode }

  //milliseconds since the epoch
  def timestamp : Long = value.getTime

  def toCalendar(implicit tz: TimeZone) = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(value)
    cal
  }
  override def toString = {
    value.toString
  }

  def toString(fmt : String)(implicit tz : TimeZone) : String = {
    val cal = toCalendar(tz)
    val sdfmt = new SimpleDateFormat(fmt)
    sdfmt.setCalendar(cal)
    sdfmt.format(cal.getTime)
  }
}

/**
* represents a closed interval of time.
*/
case class DateRange(val start : RichDate, val end : RichDate) {
  import DateOps._
  /**
  * shift this by the given unit
  */
  def +(timespan : Duration) = DateRange(start + timespan, end + timespan)
  def -(timespan : Duration) = DateRange(start - timespan, end - timespan)

  def isBefore(d : RichDate) = end < d
  def isAfter(d : RichDate) = d < start
  /**
  * make the range wider by delta on each side.  Good to catch events which
  * might spill over.
  */
  def embiggen(delta : Duration) = DateRange(start - delta, end + delta)
  /**
  * Extend the length by moving the end. We can keep the party going, but we
  * can't start it earlier.
  */
  def extend(delta : Duration) = DateRange(start, end + delta)

  def contains(point : RichDate) = (start <= point) && (point <= end)
  /**
  * Is the given Date range a (non-strict) subset of the given range
  */
  def contains(dr : DateRange) = start <= dr.start && dr.end <= end

  /**
   * produce a contiguous non-overlapping set of DateRanges
   * whose union is equivalent to this.
   * If it is passed an hour, day, or week interval, the break points
   * are set by the start timezone, otherwise we break and start + k * span.
   */
  def each(span : Duration)(implicit tz: TimeZone) : Iterable[DateRange] = {
    //tail recursive method which produces output (as a stack, so it is
    //reversed). acc is the accumulated list so far:
    @tailrec def eachRec(acc : List[DateRange], nextDr : DateRange) : List[DateRange] = {
      val next_start = span match {
        case Hours(_) => nextDr.start.earliestInHour(tz) + span
        case _ => nextDr.start + span
      }
      //the smallest grain of time we count is 1 millisecond
      val this_end = next_start - Millisecs(1)
      if( nextDr.end <= this_end ) {
        //This is the last block, output and end:
        nextDr :: acc
      }
      else {
        //Put today's portion, and then start on tomorrow:
        val today = DateRange(nextDr.start, this_end)
        eachRec(today :: acc, DateRange(next_start, nextDr.end))
      }
    }
    //have to reverse because eachDayRec produces backwards
    eachRec(Nil, this).reverse
  }
  /**
   * produce a contiguous non-overlapping set of DateRanges
   * whose union is equivalent to this.
   * Operate on CalendarDurations
   */
  def each(span : CalendarDuration)(implicit tz: TimeZone) : Iterable[DateRange] = {
    //tail recursive method which produces output (as a stack, so it is
    //reversed). acc is the accumulated list so far:
    @tailrec def eachRec(acc : List[DateRange], nextDr : DateRange) : List[DateRange] = {
      val next_start = span match {
        case Weeks(_) => nextDr.start.earliestInWeek(tz) + span
        case Days(_) => nextDr.start.earliestInDay(tz) + span
      }
      //the smallest grain of time we count is 1 millisecond
      val this_end = next_start - Millisecs(1)
      if( nextDr.end <= this_end ) {
        //This is the last block, output and end:
        nextDr :: acc
      }
      else {
        //Put today's portion, and then start on tomorrow:
        val today = DateRange(nextDr.start, this_end)
        eachRec(today :: acc, DateRange(next_start, nextDr.end))
      }
    }
    //have to reverse because eachDayRec produces backwards
    eachRec(Nil, this).reverse
  }
}
