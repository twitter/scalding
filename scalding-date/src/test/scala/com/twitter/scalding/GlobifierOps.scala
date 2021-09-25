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
import scala.util.{ Try, Success, Failure }

case class GlobifierOps(implicit tz: TimeZone, dp: DateParser) {
  val yearMonthDayHourDurations = List(Years(1), Months(1), Days(1), Hours(1))
  val yearMonthDayHourPattern = "/%1$tY/%1$tm/%1$td/%1$tH"
  private val hourlyGlobifier = Globifier(yearMonthDayHourPattern)

  def normalizeHrDr(a: DateRange) =
    DateRange(Hours(1).floorOf(a.start), Hours(1).floorOf(a.end))

  def hourlyRtGlobifier(inputDR: DateRange): DateRange =
    rtGlobifier(hourlyGlobifier, yearMonthDayHourDurations)(inputDR)

  val yearMonthDayDurations = List(Years(1), Months(1), Days(1))
  val yearMonthDayPattern = "/%1$tY/%1$tm/%1$td"
  private val dailyGlobifier = Globifier(yearMonthDayPattern)

  def normalizeDayDr(a: DateRange) =
    DateRange(Days(1).floorOf(a.start), Days(1).floorOf(a.end))

  def dailyRtGlobifier(inputDR: DateRange): DateRange =
    rtGlobifier(dailyGlobifier, yearMonthDayDurations)(inputDR)

  def rtGlobifier(globifier: Globifier, durationList: List[Duration])(inputDr: DateRange): DateRange = {
    val p = globifier.globify(inputDr)

    val drList = p.map { pattern =>
      val (lists, _, _) = pattern.split("/").tail.foldLeft((List[(Duration, Duration)](), durationList, true)) {
        case ((durationLists, mappings, shouldContinue), current) =>
          val curMapping = mappings.head
          if (shouldContinue) {
            val tryDuration: Try[Duration] = Try(current.toInt).map { indx =>
              curMapping match {
                case t if mappings.tail == Nil => t
                case _ => Millisecs(0)
              }
            }

            val (duration, doContinue) = tryDuration match {
              case Success(d) => (d, true)
              case Failure(e) =>
                val dur: Duration = curMapping match {
                  case Years(_) => sys.error("Current is " + current + ", parsed as all years?")
                  case Months(_) => Years(1)
                  case Days(_) => Months(1)
                  case Hours(_) => Days(1)
                }
                (dur, false)
            }

            val base: Duration = Try(current.toInt).map { indx =>
              curMapping match {
                case Years(_) => Years(indx - 1970)
                case Months(_) => Months(indx - 1) // months and days are 1 offsets not 0
                case Days(_) => Days(indx - 1)
                case Hours(_) => Hours(indx)
              }
            }.getOrElse(Hours(0))
            (durationLists :+ (base, duration), mappings.tail, doContinue)
          } else {
            (durationLists, mappings.tail, false)
          }
      }
      val baseDate = lists.foldLeft(RichDate("1970-01-01T00")) {
        case (curDate, (base, _)) =>
          base.addTo(curDate)
      }
      val endDate = lists.foldLeft(baseDate) {
        case (curDate, (_, dur)) =>
          dur.addTo(curDate)
      }
      DateRange(baseDate, endDate - Millisecs(1))
    }.sortBy(_.start)

    def combineDR(existing: DateRange, next: DateRange): DateRange = {
      require(existing.end == next.start - Millisecs(1), "Not contigious range: \n" + existing + "\n" + next + "...From:\n" + p.mkString(",\n"))
      DateRange(existing.start, next.end)
    }

    drList.reduceLeft(combineDR)
  }
}
