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
import java.util.regex.Pattern

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
* calField should be a java.util.Calendar field
*/
@serializable
object Duration {
  val SEC_IN_MS = 1000
  val MIN_IN_MS = 60 * SEC_IN_MS
  val HOUR_IN_MS = 60 * MIN_IN_MS
  val UTC_UNITS = List((Hours,HOUR_IN_MS),(Minutes,MIN_IN_MS),(Seconds,SEC_IN_MS),(Millisecs,1))

  // Creates the largest unit corresponding to this number of milliseconds (or possibly a duration list)
  def fromMillisecs(diffInMs : Long) : AbsoluteDuration = {
    // Try to see if we have an even number of any "calendar-free" units
    // Note, no unit is truly calendar free due to leap years, seconds, etc... so
    // so this is approximate
    // We can't fail the last one, x % 1 == 0
    UTC_UNITS.find { u_ms : (Function1[Int,AbsoluteDuration],Int) =>
        //This always returns something, maybe the last item:
        (diffInMs % u_ms._2) == 0
    }.map { u_ms : (Function1[Int,AbsoluteDuration],Int) =>
      val count = diffInMs / u_ms._2
      if (count <= Int.MaxValue)
        u_ms._1(count.toInt)
      else {
        if (diffInMs / HOUR_IN_MS > Int.MaxValue) throw new RuntimeException("difference not expressable in 2^{31} hours")

        AbsoluteDurationList(UTC_UNITS.foldLeft((diffInMs, Nil : List[AbsoluteDuration])) { (state, u_ms) =>
          val (rest, durList) = state
          val (constructor, timeInterval) = u_ms
          val thisCnt = (rest / timeInterval).toInt
          val next = rest - (thisCnt) * timeInterval
          (next, constructor(thisCnt) :: durList)
        }._2)
      }
    //This get should never throw because the list always finds something
    }.get
  }
}

@serializable
abstract class Duration(val calField : Int, val count : Int, val tz : TimeZone) {
  protected def calAdd(that : RichDate, steps : Int) = {
    val cal = that.toCalendar(tz)
    cal.setLenient(true)
    cal.add(calField, steps)
    new RichDate(cal.getTime)
  }

  def addTo(that : RichDate) = calAdd(that, count)

  def subtractFrom(that : RichDate) = calAdd(that, -count)

  // Return the latest RichDate at boundary of this time unit, ignoring
  // the count of the units.  Like a truncation.
  // Only makes sense for non-mixed durations.
  def floorOf(that : RichDate) : RichDate = {
    val cal = that.toCalendar(tz)
    RichDate(DateUtils.truncate(cal, calField).getTime)
  }
}

/*
 * These are reasonably indepedendent of calendars (or we will pretend)
 */
@serializable
object AbsoluteDuration {
  def max(a : AbsoluteDuration, b : AbsoluteDuration) = if(a > b) a else b
}
trait AbsoluteDuration extends Duration with Ordered[AbsoluteDuration] {
  def toSeconds : Double = {
    calField match {
      case Calendar.MILLISECOND => count / 1000.0
      case Calendar.SECOND => count.toDouble
      case Calendar.MINUTE => count * 60.0
      case Calendar.HOUR => count * 60.0 * 60.0
    }
  }
  def toMillisecs : Long = {
    calField match {
      case Calendar.MILLISECOND => count.toLong
      case Calendar.SECOND => count.toLong * 1000L
      case Calendar.MINUTE => count.toLong * 1000L * 60L
      case Calendar.HOUR => count.toLong * 1000L * 60L * 60L
    }
  }
  def compare(that : AbsoluteDuration) : Int = {
    this.toMillisecs.compareTo(that.toMillisecs)
  }
  def +(that : AbsoluteDuration) = {
    Duration.fromMillisecs(this.toMillisecs + that.toMillisecs)
  }
}

case class Millisecs(cnt : Int) extends Duration(Calendar.MILLISECOND, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Seconds(cnt : Int) extends Duration(Calendar.SECOND, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Minutes(cnt : Int) extends Duration(Calendar.MINUTE, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Hours(cnt : Int) extends Duration(Calendar.HOUR, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Days(cnt : Int)(implicit tz : TimeZone)
  extends Duration(Calendar.DAY_OF_MONTH, cnt, tz)

case class Weeks(cnt : Int)(implicit tz : TimeZone)
  extends Duration(Calendar.WEEK_OF_YEAR, cnt, tz) {

  // The library we are using can't handle week truncation...
  override def floorOf(that : RichDate) = {
    val step = Days(1)
    @tailrec def recentMonday(rd : RichDate) : RichDate = {
      rd.toCalendar(tz).get(Calendar.DAY_OF_WEEK) match {
        case Calendar.MONDAY => rd
        case _ => recentMonday(step.subtractFrom(rd))
      }
    }
    //Set it to the earliest point in the day:
    step.floorOf(recentMonday(that))
  }
}

case class Months(cnt : Int)(implicit tz : TimeZone)
  extends Duration(Calendar.MONTH, cnt, tz)

case class Years(cnt : Int)(implicit tz : TimeZone)
  extends Duration(Calendar.YEAR, cnt, tz)

abstract class AbstractDurationList[T <: Duration](parts : List[T]) extends Duration(-1,-1, null) {
  override def addTo(that : RichDate) = {
    parts.foldLeft(that) { (curdate, next) => next.addTo(curdate) }
  }
  override def subtractFrom(that : RichDate) = {
    parts.foldLeft(that) { (curdate, next) => next.subtractFrom(curdate) }
  }
  //This does not make sense for a DurationList interval, pass through
  override def floorOf(that : RichDate) = that
}

case class DurationList(parts : List[Duration]) extends AbstractDurationList[Duration](parts)

case class AbsoluteDurationList(parts : List[AbsoluteDuration])
  extends AbstractDurationList[AbsoluteDuration](parts) with AbsoluteDuration {
  override def toSeconds = parts.map{ _.toSeconds }.sum
  override def toMillisecs : Long = parts.map{ _.toMillisecs }.sum
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
  def +(interval : Duration) = interval.addTo(this)
  def -(interval : Duration) = interval.subtractFrom(this)

  //Inverse of the above, d2 + (d1 - d2) == d1
  def -(that : RichDate) = Duration.fromMillisecs(value.getTime - that.value.getTime)

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
   * If it is passed an integral unit of time (not a DurationList), it stops at boundaries
   * which are set by the start timezone, else break at start + k * span.
   */
  def each(span : Duration) : Iterable[DateRange] = {
    //tail recursive method which produces output (as a stack, so it is
    //reversed). acc is the accumulated list so far:
    @tailrec def eachRec(acc : List[DateRange], nextDr : DateRange) : List[DateRange] = {
      val next_start = span.floorOf(nextDr.start) + span
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

/*
 * All the Globification logic is encoded in this one
 * class.  It has a list of child ranges that share boundaries
 * with the current range and are completely contained within
 * current range.  This children must be ordered from largest
 * to smallest in size.
 */
@serializable
class BaseGlobifier(dur : Duration, val sym: String, pattern : String, tz : TimeZone, child : Option[BaseGlobifier]) {
  import DateOps._
  // result <= rd
  private def greatestLowerBound(rd : RichDate) = dur.floorOf(rd)
  // rd <= result
  private def leastUpperBound(rd : RichDate) : RichDate = {
    val lb = greatestLowerBound(rd)
    if (lb == rd)
      rd
    else
      lb + dur
  }

  def format(rd : RichDate) : String = String.format(pattern, rd.toCalendar(tz))

  // Generate a lazy list of all children
  final def children : Stream[BaseGlobifier] = child match {
    case Some(c) => Stream.cons(c, c.children)
    case None => Stream.empty
  }

  final def asteriskChildren(rd : RichDate) : String = {
    val childStarPattern = children.foldLeft(pattern) { (this_pat, child) =>
      this_pat.replaceAll(Pattern.quote(child.sym), "*")
    }
    String.format(childStarPattern, rd.toCalendar(tz))
  }

  // Handles the case of zero interior boundaries
  // with potential boundaries only at the end points.
  private def simpleCase(dr : DateRange) : List[String] = {
    val sstr = format(dr.start)
    val estr = format(dr.end)
    if (dr.end < dr.start) {
      Nil
    }
    else if (child.isEmpty) {
      //There is only one block:
      assert(sstr == estr, "Malformed heirarchy" + sstr + " != " + estr)
      List(sstr)
    }
    else {
      /*
       * Two cases: we should asterisk our children, or we need
       * to recurse.  If we fill this entire range, just asterisk,
       */
      val bottom = children.last
      val fillsright = format(leastUpperBound(dr.end)) ==
        format(bottom.leastUpperBound(dr.end))
      val fillsleft = format(greatestLowerBound(dr.start)) ==
        format(bottom.greatestLowerBound(dr.start))
      if (fillsright && fillsleft) {
        List(asteriskChildren(dr.start))
      }
      else {
        child.get.globify(dr)
      }
    }
  }

  def globify(dr : DateRange) : List[String] = {
    /* We know:
     * start <= end : by assumption
     * mid1 - start < delta : mid1 is least upper bound
     * end - mid2 < delta : mid2 is greatest lower bound
     * mid1 = mid2 + n*delta : both on the boundary.
     * if mid1 <= mid2, then we contain a boundary point,
     * else we do not.
     */
    val mid1 = leastUpperBound(dr.start)
    val mid2 = greatestLowerBound(dr.end)
    //Imprecise patterns may not need to drill down, let's see if we can stop early:
    val sstr = format(dr.start)
    val estr = format(dr.end)
    if (sstr == estr) {
      List(sstr)
    }
    else if (dr.end < dr.start) {
      //This is nonsense:
      Nil
    }
    else if (mid2 < mid1) {
      //We do not contain a boundary point:
      simpleCase(dr)
    }
    // otherwise we contain one or more than one boundary points
    else if (mid1 == mid2) {
      //we contain exactly one boundary point:
      simpleCase(DateRange(dr.start, mid1 - Millisecs(1))) ++
        simpleCase(DateRange(mid1, dr.end))
    }
    else {
      //We contain 2 or more boundary points:
      // [start <= mid1 < mid2 <= end]
      // First check to see if we even need to check our children:
      simpleCase(DateRange(dr.start, mid1 - Millisecs(1))) ++
        (asteriskChildren(mid1) ::
        globify(DateRange(mid1 + dur, dr.end)))
    }
  }
}

case class HourGlob(pat : String)(implicit tz : TimeZone)
  extends BaseGlobifier(Hours(1),"%1$tH", pat, tz, None)

case class DayGlob(pat : String)(implicit tz: TimeZone)
  extends BaseGlobifier(Days(1)(tz), "%1$td", pat, tz, Some(HourGlob(pat)))

case class MonthGlob(pat : String)(implicit tz: TimeZone)
  extends BaseGlobifier(Months(1)(tz), "%1$tm", pat, tz, Some(DayGlob(pat)))

/*
 * This is the outermost globifier and should generally be used to globify
 */
@serializable
case class Globifier(pat : String)(implicit tz: TimeZone)
  extends BaseGlobifier(Years(1)(tz), "%1$tY", pat, tz, Some(MonthGlob(pat)))
