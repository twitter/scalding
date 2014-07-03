package com.twitter.scalding

import java.text.SimpleDateFormat
import java.util._

import org.specs._

class CalendarOpsTest extends Specification {
  noDetailedDiffs()

  val cal = Calendar.getInstance();

  val dateParser = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH);
  val dateTimeParser = new SimpleDateFormat("MMM dd, yyyy H:mm:ss.SSS", Locale.ENGLISH);

  "The CalendarOps truncate method" should {
    "not truncate if the specified field is milliseconds" in {
      cal.setTime(new Date(1384819200555L))
      cal.get(Calendar.MILLISECOND) must be equalTo 555
    }

    "truncate to a year" in {
      dateParser.parse("January 1, 2002") must be equalTo
        CalendarOps.truncate(dateParser.parse("February 12, 2002 12:34:56.789"), Calendar.YEAR)

      dateParser.parse("January 1, 2001") must be equalTo
        CalendarOps.truncate(dateParser.parse("November 18, 2001 1:23:11.321"), Calendar.YEAR)
    }

    "truncate to a month" in {
      dateParser.parse("February 1, 2002") must be equalTo
        CalendarOps.truncate(dateParser.parse("February 12, 2002 12:34:56.789"), Calendar.MONTH)

      dateParser.parse("November 1, 2001") must be equalTo
        CalendarOps.truncate(dateParser.parse("November 18, 2001 1:23:11.321"), Calendar.MONTH)
    }

    "truncate to a date" in {
      dateParser.parse("February 12, 2002") must be equalTo
        CalendarOps.truncate(dateParser.parse("February 12, 2002 12:34:56.789"), Calendar.DATE)

      dateParser.parse("November 18, 2001") must be equalTo
        CalendarOps.truncate(dateParser.parse("November 18, 2001 1:23:11.321"), Calendar.DATE)
    }

    "truncate to a minute" in {
      dateTimeParser.parse("February 12, 2002 12:34:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("February 12, 2002 12:34:56.789"), Calendar.MINUTE)

      dateTimeParser.parse("November 18, 2001 1:23:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("November 18, 2001 1:23:11.321"), Calendar.MINUTE)
    }

    "truncate to a second" in {
      dateTimeParser.parse("February 12, 2002 12:34:56.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("February 12, 2002 12:34:56.789"), Calendar.SECOND)

      dateTimeParser.parse("November 18, 2001 1:23:11.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("November 18, 2001 1:23:11.321"), Calendar.SECOND)
    }

    "truncate to AM" in {
      dateTimeParser.parse("February 3, 2002 00:00:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 01:10:00.000"), Calendar.AM_PM)

      dateTimeParser.parse("February 3, 2002 00:00:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 11:10:00.000"), Calendar.AM_PM)
    }

    "truncate to PM" in {
      dateTimeParser.parse("February 3, 2002 12:00:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 13:10:00.000"), Calendar.AM_PM)

      dateTimeParser.parse("February 3, 2002 12:00:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("February 3, 2002 19:10:00.000"), Calendar.AM_PM)
    }

    "truncate respects DST" in {
      TimeZone.setDefault(TimeZone.getTimeZone("MET"))
      dateTimeParser.setTimeZone(TimeZone.getTimeZone("MET"))

      dateTimeParser.parse("March 30, 2003 00:00:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("March 30, 2003 05:30:45.000"), Calendar.DATE)

      dateTimeParser.parse("October 26, 2003 00:00:00.000") must be equalTo
        CalendarOps.truncate(dateTimeParser.parse("October 26, 2003 05:30:45.000"), Calendar.DATE)
    }
  }
}
