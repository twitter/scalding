package com.twitter.scalding

import java.util.{ Date, Calendar }
import scala.annotation.tailrec

/**
 *
 */
object CalendarOps {
  def truncate(date: Calendar, field: Int): Calendar = {
    @tailrec
    def truncateIter(cal: Calendar, field: Int, currentField: Int): Calendar = {
      if (currentField > field) {
        currentField match {
          case Calendar.DAY_OF_MONTH => cal.set(currentField, 1)
          case Calendar.DAY_OF_WEEK_IN_MONTH |
            Calendar.DAY_OF_WEEK |
            Calendar.DAY_OF_YEAR |
            Calendar.WEEK_OF_MONTH |
            Calendar.WEEK_OF_YEAR |
            Calendar.HOUR_OF_DAY => () // Skip
          case _ => cal.set(currentField, 0)
        }

        truncateIter(cal, field, currentField - 1)
      } else {
        cal
      }
    }

    val cloned = date.clone().asInstanceOf[Calendar]

    truncateIter(cloned, field, Calendar.MILLISECOND)
  }

  def truncate(date: Date, field: Int): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(date)

    truncate(cal, field).getTime()
  }

}
