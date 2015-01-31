package com.twitter.scalding

import java.text.SimpleDateFormat
import java.util._

import org.scalatest.WordSpec

class CalendarOpsTest extends WordSpec {
  val cal = Calendar.getInstance()

  val dateParser = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH)
  val dateTimeParser = new SimpleDateFormat("MMM dd, yyyy H:mm:ss.SSS", Locale.ENGLISH)

  "The CalendarOps truncate method" should {
    "not truncate if the specified field is milliseconds" in {
      cal.setTime(new Date(1384819200555L))
      assert(cal.get(Calendar.MILLISECOND) === 555)
    }

    "truncate to a year" in {
      assert(dateParser.parse("January 1, 2002") ===
        CalendarOps.truncate(dateParser.parse("February 12, 2002 12:34:56.789"), Calendar.YEAR))

      assert(dateParser.parse("January 1, 2001") ===
        CalendarOps.truncate(dateParser.parse("November 18, 2001 1:23:11.321"), Calendar.YEAR))
    }

    "truncate to a month" in {
      assert(dateParser.parse("February 1, 2002") ===
        CalendarOps.truncate(dateParser.parse("February 12, 2002 12:34:56.789"), Calendar.MONTH))

      assert(dateParser.parse("November 1, 2001") ===
        CalendarOps.truncate(dateParser.parse("November 18, 2001 1:23:11.321"), Calendar.MONTH))
    }

    "truncate to a date" in {
      assert(dateParser.parse("February 12, 2002") ==
        CalendarOps.truncate(dateParser.parse("February 12, 2002 12:34:56.789"), Calendar.DATE))

      assert(dateParser.parse("November 18, 2001") ===
        CalendarOps.truncate(dateParser.parse("November 18, 2001 1:23:11.321"), Calendar.DATE))
    }

    "truncate to a minute" in {
      assert(dateTimeParser.parse("February 12, 2002 12:34:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("February 12, 2002 12:34:56.789"), Calendar.MINUTE))

      assert(dateTimeParser.parse("November 18, 2001 1:23:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("November 18, 2001 1:23:11.321"), Calendar.MINUTE))
    }

    "truncate to a second" in {
      assert(dateTimeParser.parse("February 12, 2002 12:34:56.000") ===
        CalendarOps.truncate(dateTimeParser.parse("February 12, 2002 12:34:56.789"), Calendar.SECOND))

      assert(dateTimeParser.parse("November 18, 2001 1:23:11.000") ===
        CalendarOps.truncate(dateTimeParser.parse("November 18, 2001 1:23:11.321"), Calendar.SECOND))
    }

    "truncate to AM" in {
      assert(dateTimeParser.parse("February 3, 2002 00:00:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 01:10:00.000"), Calendar.AM_PM))

      assert(dateTimeParser.parse("February 3, 2002 00:00:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 11:10:00.000"), Calendar.AM_PM))
    }

    "truncate to PM" in {
      assert(dateTimeParser.parse("February 3, 2002 12:00:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 13:10:00.000"), Calendar.AM_PM))

      assert(dateTimeParser.parse("February 3, 2002 12:00:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 19:10:00.000"), Calendar.AM_PM))
    }

    "truncate respects DST" in {
      TimeZone.setDefault(TimeZone.getTimeZone("MET"))
      dateTimeParser.setTimeZone(TimeZone.getTimeZone("MET"))

      assert(dateTimeParser.parse("March 30, 2003 00:00:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("March 30, 2003 05:30:45.000"), Calendar.DATE))

      assert(dateTimeParser.parse("October 26, 2003 00:00:00.000") ===
        CalendarOps.truncate(dateTimeParser.parse("October 26, 2003 05:30:45.000"), Calendar.DATE))
    }
  }
}
