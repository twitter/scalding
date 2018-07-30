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

import java.util.TimeZone

object DateRange extends java.io.Serializable {
  /**
   * Parse this string into a range.
   * 2009-10-01 is interpetted as the whole day
   * 2009-10-01T12 is interpetted as the whole hour
   * 2009-10-01T12:00 is interpetted as a single minute
   * 2009-10-01T12:00:02 is interpretted as a single second
   *
   * This is called parse to avoid a collision with implicit conversions
   * from String to RichDate
   */
  def parse(truncatediso8601: String)(implicit tz: TimeZone, dp: DateParser): DateRange =
    DateRange(RichDate(truncatediso8601), RichDate.upperBound(truncatediso8601))

  /**
   * We take the upper bound of the second parameter, so we take the latest time that
   * could be construed as matching the string passed, e.g.
   * ("2011-01-02T04", "2011-01-02T05") includes two full hours (all of 4 and all of 5)
   */
  def parse(iso8601start: String,
    iso8601inclusiveUpper: String)(implicit tz: TimeZone, dp: DateParser): DateRange = {

    val start = RichDate(iso8601start)
    val end = RichDate.upperBound(iso8601inclusiveUpper)
    //Make sure the end is not before the beginning:
    assert(start <= end, "end of date range must occur after the start")
    DateRange(start, end)
  }

  /**
   * Pass one or two args (from a scalding.Args .list) to parse into a DateRange
   */
  def parse(fromArgs: Seq[String])(implicit tz: TimeZone, dp: DateParser): DateRange = fromArgs match {
    case Seq(s, e) => parse(s, e)
    case Seq(o) => parse(o)
    case x => sys.error("--date must have exactly one or two date[time]s. Got: " + x.toString)
  }

  /**
   * DateRanges are inclusive. Use this to create a DateRange that excludes
   * the last millisecond from the second argument.
   */
  def exclusiveUpper(include: RichDate, exclude: RichDate): DateRange =
    DateRange(include, exclude - Millisecs(1))
}

/**
 * represents a closed interval of time.
 *
 * TODO: This should be Range[RichDate, Duration] for an appropriate notion
 * of Range
 */
case class DateRange(val start: RichDate, val end: RichDate) {
  require(start <= end, s"""The start "${start}" must be before or on the end "${end}".""")
  /**
   * shift this by the given unit
   */
  def +(timespan: Duration) = DateRange(start + timespan, end + timespan)
  def -(timespan: Duration) = DateRange(start - timespan, end - timespan)

  def isBefore(d: RichDate) = end < d
  def isAfter(d: RichDate) = d < start
  /**
   * make the range wider by delta on each side.  Good to catch events which
   * might spill over.
   */
  def embiggen(delta: Duration) = DateRange(start - delta, end + delta)
  /**
   * Extend the length by moving the end. We can keep the party going, but we
   * can't start it earlier.
   */
  def extend(delta: Duration) = DateRange(start, end + delta)

  /**
   * Extend the length by moving the start.
   * Turns out, we can start the party early.
   */
  def prepend(delta: Duration) = DateRange(start - delta, end)

  def contains(point: RichDate) = (start <= point) && (point <= end)
  /**
   * Is the given Date range a (non-strict) subset of the given range
   */
  def contains(dr: DateRange) = start <= dr.start && dr.end <= end

  /**
   * produce a contiguous non-overlapping set of DateRanges
   * whose union is equivalent to this.
   * If it is passed an integral unit of time (not a DurationList), it stops at boundaries
   * which are set by the start timezone, else break at start + k * span.
   */
  def each(span: Duration): Iterable[DateRange] = {
    //tail recursive method which produces output (as a stack, so it is
    //reversed). acc is the accumulated list so far:
    @tailrec def eachRec(acc: List[DateRange], nextDr: DateRange): List[DateRange] = {
      val next_start = span.floorOf(nextDr.start) + span
      //the smallest grain of time we count is 1 millisecond
      val this_end = next_start - Millisecs(1)
      if (nextDr.end <= this_end) {
        //This is the last block, output and end:
        nextDr :: acc
      } else {
        //Put today's portion, and then start on tomorrow:
        val today = DateRange(nextDr.start, this_end)
        eachRec(today :: acc, DateRange(next_start, nextDr.end))
      }
    }
    //have to reverse because eachDayRec produces backwards
    eachRec(Nil, this).reverse
  }

  def length: AbsoluteDuration =
    AbsoluteDuration.fromMillisecs(end.timestamp - start.timestamp + 1L)
}
