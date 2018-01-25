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

import org.scalatest.WordSpec
import java.util.Calendar
import java.util.TimeZone

class DateTest extends WordSpec {
  implicit val tz: TimeZone = DateOps.PACIFIC

  implicit def dateParser: DateParser = DateParser.default

  "A RichDate" should {
    "implicitly convert strings" in {
      val rd1: RichDate = "2011-10-20"
      val rd2: RichDate = "2011-10-20"
      val rd3: RichDate = "20111020"
      val rd4: RichDate = "20111020"
      assert(rd1 === rd2)
      assert(rd3 === rd4)
      assert(rd1 === rd3)
    }
    "implicitly convert calendars" in {
      val rd1WithoutDash: RichDate = "20111020"
      val calWithoutDash = Calendar.getInstance(tz)
      calWithoutDash.setTime(rd1WithoutDash.value)
      val rd2WithoutDash: RichDate = calWithoutDash

      val rd1: RichDate = "2011-10-20"
      val cal = Calendar.getInstance(tz)
      cal.setTime(rd1.value)
      val rd2: RichDate = cal

      assert(rd1WithoutDash == rd2WithoutDash)
      assert(rd1 === rd2)
    }
    "deal with strings with spaces" in {
      val rd1: RichDate = "   2011-10-20 "
      val rd2: RichDate = "2011-10-20 "
      val rd3: RichDate = " 2011-10-20     "
      assert(rd1 === rd2)
      assert(rd1 === rd3)
    }
    "handle dates with slashes and underscores" in {
      val rd1: RichDate = "2011-10-20"
      val rd2: RichDate = "2011/10/20"
      val rd3: RichDate = "2011_10_20"
      assert(rd1 === rd2)
      assert(rd1 === rd3)
    }
    "be able to parse milliseconds" in {
      val rd1: RichDate = "2011-10-20 20:01:11.0"
      val rd2: RichDate = "2011-10-20   22:11:24.23"
      val rd3: RichDate = "2011-10-20 22:11:24.023    "
      assert(rd2 === rd3)
    }
    "throw an exception when trying to parse illegal strings" in {
      // Natty is *really* generous about what it accepts
      intercept[IllegalArgumentException] { RichDate("jhbjhvhjv") }
      intercept[IllegalArgumentException] { RichDate("99-99-99") }
    }
    "be able to deal with arithmetic operations with whitespace" in {
      val rd1: RichDate = RichDate("2010-10-02") + Seconds(1)
      val rd2: RichDate = "  2010-10-02  T  00:00:01    "
      assert(rd1 === rd2)
    }
    "be able to deal with arithmetic operations without hyphens and whitespaces" in {
      val rd1: RichDate = RichDate("20101002") + Seconds(1)
      val rd2: RichDate = "  2010-10-02  T  00:00:01    "
      assert(rd1 === rd2)
    }
    "Have same equals & hashCode as Date (crazy?)" in {
      val rd1: RichDate = "2011-10-20"
      assert(rd1 === rd1.value)
      assert(rd1.hashCode === rd1.value.hashCode)
    }
    "be well ordered" in {
      val rd1: RichDate = "2011-10-20"
      val rd2: RichDate = "2011-10-21"
      assert(rd1 < rd2)
      assert(rd1 <= rd2)
      assert(rd2 > rd1)
      assert(rd2 >= rd1)
      assert(rd1 >= rd1)
      assert(rd2 >= rd2)
    }
    "be able to compare with before() and after() with TimeZone in context" in {
      implicit val tz: TimeZone = TimeZone.getDefault
      val rd1: RichDate = "2011-01-01"
      val rd2: RichDate = "2012-01-01"
      assert(rd1.before(rd2))
    }
    "implicitly convert from long" in {
      // This kind of implicit is not safe (what does the long mean?)
      implicit def longToDate(l: Long): RichDate = RichDate(l)

      //This is close to: Mon Oct 24 20:03:13 PDT 2011
      val long_val = 1319511818135L
      val rd1 = "2011-10-24T20:03:00"
      val rd2 = "2011-10-24T20:04:00"
      assert(DateRange(rd1, rd2).contains(RichDate(long_val)))
      //Check edge cases:
      assert(DateRange(rd1, long_val).contains(long_val))
      assert(DateRange(rd1, (long_val + 1)).contains(long_val))
      assert(DateRange(long_val, rd2).contains(long_val))
      assert(DateRange((long_val - 1), rd2).contains(long_val))

      assert(!DateRange(rd1, "2011-10-24T20:03:01").contains(long_val))
      assert(!DateRange(rd1, (long_val - 1)).contains(long_val))
      assert(!DateRange((long_val + 1), rd2).contains(long_val))
    }
    "roundtrip successfully" in {
      val start_str = "2011-10-24 20:03:00"
      //string -> date -> string
      assert(RichDate(start_str).toString(DateOps.DATETIME_HMS_WITH_DASH) === start_str)
      //long -> date == date -> long -> date
      val long_val = 1319511818135L
      val date = RichDate(long_val)
      val long2 = date.value.getTime
      val date2 = RichDate(long2)
      assert(date === date2)
      assert(long_val === long2)
    }
    "know the most recent time units" in {
      //10-25 is a Tuesday, earliest in week is a monday
      assert(Weeks(1).floorOf("2011-10-25") === RichDate("2011-10-24"))
      assert(Weeks(1).floorOf("20111025") === RichDate("2011-10-24"))
      assert(Days(1).floorOf("2011-10-25 10:01") === RichDate("2011-10-25 00:00"))
      assert(Days(1).floorOf("201110251001") === RichDate("2011-10-25 00:00"))
      //Leaving off the time should give the same result:
      assert(Days(1).floorOf("201110251001") === RichDate("2011-10-25"))
      assert(Days(1).floorOf("2011-10-25 10:01") === RichDate("2011-10-25"))
      assert(Hours(1).floorOf("201110251001") === RichDate("2011-10-25 10:00"))
      assert(Hours(1).floorOf("2011-10-25 10:01") === RichDate("2011-10-25 10:00"))
    }
    "correctly do arithmetic" in {
      val d1: RichDate = "2011-10-24"
      (-4 to 4).foreach { n =>
        List(Hours, Minutes, Seconds, Millisecs).foreach { u =>
          val d2 = d1 + u(n)
          assert((d2 - d1) === u(n))
        }
      }
    }
    "correctly calculate upperBound" in {
      assert(Seconds(1).floorOf(RichDate.upperBound("20101001")) === Seconds(1).floorOf(RichDate("2010-10-01 23:59:59")))
      assert(Seconds(1).floorOf(RichDate.upperBound("2010100114")) === Seconds(1).floorOf(RichDate("2010-10-01 14:59:59")))
      assert(Seconds(1).floorOf(RichDate.upperBound("201010011415")) === Seconds(1).floorOf(RichDate("2010-10-01 14:15:59")))
      assert(Seconds(1).floorOf(RichDate.upperBound("2010-10-01")) === Seconds(1).floorOf(RichDate("2010-10-01 23:59:59")))
      assert(Seconds(1).floorOf(RichDate.upperBound("2010-10-01 14")) === Seconds(1).floorOf(RichDate("2010-10-01 14:59:59")))
      assert(Seconds(1).floorOf(RichDate.upperBound("2010-10-01 14:15")) === Seconds(1).floorOf(RichDate("2010-10-01 14:15:59")))
    }
    "Have an implicit Ordering" in {
      implicitly[Ordering[RichDate]]
      implicitly[Ordering[(String, RichDate)]]
    }
  }
  "A DateRange" should {
    "correctly iterate on each duration" in {
      def rangeContainTest(d1: DateRange, dur: Duration) = {
        assert(d1.each(dur).forall((d1r: DateRange) => d1.contains(d1r)))
      }
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Weeks(1))
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Weeks(2))
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Days(1))
      //Prime non one:
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Days(5))
      //Prime number of Minutes
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Minutes(13))
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Hours(13))
      assert(DateRange("2010-10-01", "2010-10-10").each(Days(1)).size === 10)
      assert(DateRange("201010010000", RichDate("2010-10-02") - Millisecs(1)).each(Hours(1)).size === 24)
      assert(DateRange("2010-10-01 00:00", RichDate("2010-10-02") - Millisecs(1)).each(Hours(1)).size === 24)
      assert(DateRange("2010-10-01 00:00", RichDate("2010-10-02") + Millisecs(1)).each(Hours(1)).size === 25)
      assert(DateRange("2010-10-01", RichDate.upperBound("2010-10-20")).each(Days(1)).size === 20)
      assert(DateRange("2010-10-01", RichDate.upperBound("2010-10-01")).each(Hours(1)).size === 24)
      assert(DateRange("2010-10-31", RichDate.upperBound("2010-10-31")).each(Hours(1)).size === 24)
      assert(DateRange("2010-10-31", RichDate.upperBound("2010-10-31")).each(Days(1)).size === 1)
      assert(DateRange("2010-10-31 12:00", RichDate.upperBound("2010-10-31 13")).each(Minutes(1)).size === 120)
    }
    "have each partition disjoint and adjacent" in {
      def eachIsDisjoint(d: DateRange, dur: Duration): Unit = {
        val dl = d.each(dur)
        assert(dl.zip(dl.tail).forall {
          case (da, db) =>
            da.isBefore(db.start) && db.isAfter(da.end) && ((da.end + Millisecs(1)) == db.start)
        })
      }
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Days(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Weeks(1))
      eachIsDisjoint(DateRange("2010-10-01", "2011-10-03"), Weeks(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Months(1))
      eachIsDisjoint(DateRange("2010-10-01", "2011-10-03"), Months(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Hours(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Hours(2))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Minutes(1))
    }
    "reject an end that is before its start" in {
      intercept[IllegalArgumentException] { DateRange("2010-10-02", "2010-10-01") }
    }
  }
  "Time units" should {
    def isSame(d1: Duration, d2: Duration) = {
      (RichDate("2011-12-01") + d1) == (RichDate("2011-12-01") + d2)
    }
    "have 1000 milliseconds in a sec" in {
      assert(isSame(Millisecs(1000), Seconds(1)))
      assert(Seconds(1).toMillisecs === 1000L)
      assert(Millisecs(1000).toSeconds === 1.0)
      assert(Seconds(2).toMillisecs === 2000L)
      assert(Millisecs(2000).toSeconds === 2.0)
    }
    "have 60 seconds in a minute" in {
      assert(isSame(Seconds(60), Minutes(1)))
      assert(Minutes(1).toSeconds === 60.0)
      assert(Minutes(1).toMillisecs === 60 * 1000L)
      assert(Minutes(2).toSeconds === 120.0)
      assert(Minutes(2).toMillisecs === 120 * 1000L)
    }
    "have 60 minutes in a hour" in {
      assert(isSame(Minutes(60), Hours(1)))
      assert(Hours(1).toSeconds === 60.0 * 60.0)
      assert(Hours(1).toMillisecs === 60 * 60 * 1000L)
      assert(Hours(2).toSeconds === 2 * 60.0 * 60.0)
      assert(Hours(2).toMillisecs === 2 * 60 * 60 * 1000L)
    }
    "have 7 days in a week" in { assert(isSame(Days(7), Weeks(1))) }
  }
  "AbsoluteDurations" should {
    "behave as comparable" in {
      assert(Hours(5) >= Hours(2))
      assert(Minutes(60) >= Minutes(60))
      assert(Hours(1) < Millisecs(3600001))
    }
    "add properly" in {
      assert((Hours(2) + Hours(1)).compare(Hours(3)) === 0)
    }
    "have a well behaved max function" in {
      assert(AbsoluteDuration.max(Hours(1), Hours(2)).compare(Hours(2)) === 0)
    }
  }
  "Globifiers" should {
    "handle specific hand crafted examples" in {
      val t1 = Globifier("/%1$tY/%1$tm/%1$td/%1$tH")
      val t2 = Globifier("/%1$tY/%1$tm/%1$td/")

      val testcases =
        (t1.globify(DateRange("2011-12-01T14", "2011-12-04")),
          List("/2011/12/01/14", "/2011/12/01/15", "/2011/12/01/16", "/2011/12/01/17", "/2011/12/01/18",
            "/2011/12/01/19", "/2011/12/01/20", "/2011/12/01/21", "/2011/12/01/22", "/2011/12/01/23",
            "/2011/12/02/*", "/2011/12/03/*", "/2011/12/04/00")) ::
            (t1.globify(DateRange("2011-12-01", "2011-12-01T23:59")),
              List("/2011/12/01/*")) ::
              (t1.globify(DateRange("2014-06-30T00", "2014-07-01T00")),
                List("/2014/06/30/*", "/2014/07/01/00")) ::
                (t1.globify(DateRange("2011-12-01T12", "2011-12-01T12:59")),
                  List("/2011/12/01/12")) ::
                  (t1.globify(DateRange("2011-12-01T12", "2011-12-01T14")),
                    List("/2011/12/01/12", "/2011/12/01/13", "/2011/12/01/14")) ::
                    (t2.globify(DateRange("2011-12-01T14", "2011-12-04")),
                      List("/2011/12/01/", "/2011/12/02/", "/2011/12/03/", "/2011/12/04/")) ::
                      (t2.globify(DateRange("2011-12-01", "2011-12-01T23:59")),
                        List("/2011/12/01/")) ::
                        (t2.globify(DateRange("2011-12-01T12", "2011-12-01T12:59")),
                          List("/2011/12/01/")) ::
                          (t2.globify(DateRange("2011-12-01T12", "2012-01-02T14")),
                            List("/2011/12/*/", "/2012/01/01/", "/2012/01/02/")) ::
                            (t2.globify(DateRange("2011-11-01T12", "2011-12-02T14")),
                              List("/2011/11/*/", "/2011/12/01/", "/2011/12/02/")) ::
                              Nil

      testcases.foreach { case (l, r) => assert(l === r) }
    }

    "The forward and reverser should match" in {
      val globifierOps = GlobifierOps()

      val hourlyTestCases = List(
        DateRange("2011-12-01T14", "2011-12-04"),
        DateRange("2011-12-01", "2011-12-01T23:59"),
        DateRange("2014-06-30T00", "2014-07-01T00"),
        DateRange("2011-12-01T12", "2011-12-01T12:59"),
        DateRange("2011-12-01T12", "2011-12-01T14"))

      hourlyTestCases.foreach { dr =>
        val resultantDR = globifierOps.hourlyRtGlobifier(dr)
        assert(globifierOps.normalizeHrDr(dr) === globifierOps.normalizeHrDr(resultantDR))
      }

      val dailyTestCases = List(
        DateRange("2011-12-01T14", "2011-12-04"),
        DateRange("2011-12-01", "2011-12-01T23:59"),
        DateRange("2011-12-01T12", "2011-12-01T12:59"),
        DateRange("2011-12-01T12", "2012-01-02T14"),
        DateRange("2011-11-01T12", "2011-12-02T14"))

      dailyTestCases.foreach { dr =>
        val resultantDR = globifierOps.dailyRtGlobifier(dr)
        assert(globifierOps.normalizeDayDr(dr) === globifierOps.normalizeDayDr(resultantDR))
      }
    }

    def eachElementDistinct(dates: List[String]) = dates.size == dates.toSet.size
    def globMatchesDate(glob: String)(date: String) = {
      java.util.regex.Pattern.matches(glob.replaceAll("\\*", "[0-9]*"), date)
    }
    def bruteForce(pattern: String, dr: DateRange, dur: Duration)(implicit tz: java.util.TimeZone) = {
      dr.each(dur)
        .map { (dr: DateRange) => String.format(pattern, dr.start.toCalendar(tz)) }
    }

    "handle random test cases" in {
      // This kind of implicit is not safe (what does the long mean?)
      implicit def longToDate(l: Long): RichDate = RichDate(l)
      val pattern = "/%1$tY/%1$tm/%1$td/%1$tH"
      val t1 = Globifier(pattern)

      val r = new java.util.Random()
      (0 until 100) foreach { step =>
        val start = RichDate("2011-08-03").value.getTime + r.nextInt(Int.MaxValue)
        val dr = DateRange(start, start + r.nextInt(Int.MaxValue))
        val splits = bruteForce(pattern, dr, Hours(1))
        val globed = t1.globify(dr)

        assert(eachElementDistinct(globed))
        //See that each path is matched by exactly one glob:
        assert(splits.map { path => globed.filter { globMatchesDate(_)(path) }.size }
          .forall { _ == 1 })
      }
    }
  }
}
