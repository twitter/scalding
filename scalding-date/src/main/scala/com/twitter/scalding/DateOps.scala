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
import java.text.SimpleDateFormat

import scala.util.matching.Regex

/**
 * Holds some coversion functions for dealing with strings as RichDate objects
 */
object DateOps extends java.io.Serializable {
  val PACIFIC = TimeZone.getTimeZone("America/Los_Angeles")
  val UTC = TimeZone.getTimeZone("UTC")

  val DATE_WITHOUT_DASH = "yyyyMMdd"
  val DATE_WITH_DASH = "yyyy-MM-dd"
  val DATEHOUR_WITHOUT_DASH = "yyyyMMddHH"
  val DATEHOUR_WITH_DASH = "yyyy-MM-dd HH"
  val DATETIME_WITHOUT_DASH = "yyyyMMddHHmm"
  val DATETIME_WITH_DASH = "yyyy-MM-dd HH:mm"
  val DATETIME_HMS_WITHOUT_DASH = "yyyyMMddHHmmss"
  val DATETIME_HMS_WITH_DASH = "yyyy-MM-dd HH:mm:ss"
  val DATETIME_HMSM_WITH_DASH = "yyyy-MM-dd HH:mm:ss.SSS"

  private[scalding] sealed abstract class Format(val pattern: String, val validator: Regex) {
    def matches(s: String): Boolean = validator.findFirstIn(s).isDefined
  }

  private[scalding] object Format {
    private val date = """\d{4}-\d{2}-\d{2}"""
    private val sep = """(T?|\s*)"""
    private val emptyBegin = """^\s*"""
    private val emptyEnd = """\s*$"""

    case object DATE_WITHOUT_DASH extends Format(DateOps.DATE_WITHOUT_DASH, new Regex(emptyBegin + """\d{8}""" + emptyEnd))
    case object DATE_WITH_DASH extends Format(DateOps.DATE_WITH_DASH, new Regex(emptyBegin + date + emptyEnd))
    case object DATEHOUR_WITHOUT_DASH extends Format(DateOps.DATEHOUR_WITHOUT_DASH, new Regex(emptyBegin + """\d{10}""" + emptyEnd))
    case object DATEHOUR_WITH_DASH extends Format(DateOps.DATEHOUR_WITH_DASH, new Regex(emptyBegin + date + sep + """\d\d""" + emptyEnd))
    case object DATETIME_WITHOUT_DASH extends Format(DateOps.DATETIME_WITHOUT_DASH, new Regex(emptyBegin + """\d{12}""" + emptyEnd))
    case object DATETIME_WITH_DASH extends Format(DateOps.DATETIME_WITH_DASH, new Regex(emptyBegin + date + sep + """\d\d:\d\d""" + emptyEnd))
    case object DATETIME_HMS_WITHOUT_DASH extends Format(DateOps.DATETIME_HMS_WITHOUT_DASH, new Regex(emptyBegin + """\d{14}""" + emptyEnd))
    case object DATETIME_HMS_WITH_DASH extends Format(DateOps.DATETIME_HMS_WITH_DASH, new Regex(emptyBegin + date + sep + """\d\d:\d\d:\d\d""" + emptyEnd))
    case object DATETIME_HMSM_WITH_DASH extends Format(DateOps.DATETIME_HMSM_WITH_DASH, new Regex(emptyBegin + date + sep + """\d\d:\d\d:\d\d\.\d{1,3}""" + emptyEnd))
  }

  private val prepare: String => String = { (str: String) =>
    str.replace("T", " ") //We allow T to separate dates and times, just remove it and then validate
      .replaceAll("[/_]", "-") // Allow for slashes and underscores
  }

  /**
   * Return the guessed format for this datestring
   */
  private[scalding] def getFormatObject(s: String): Option[Format] = {
    val formats: List[Format] = List(
      Format.DATE_WITH_DASH,
      Format.DATEHOUR_WITH_DASH,
      Format.DATETIME_WITH_DASH,
      Format.DATETIME_HMS_WITH_DASH,
      Format.DATETIME_HMSM_WITH_DASH,
      Format.DATE_WITHOUT_DASH,
      Format.DATEHOUR_WITHOUT_DASH,
      Format.DATETIME_WITHOUT_DASH,
      Format.DATETIME_HMS_WITHOUT_DASH)

    formats.find { _.matches(prepare(s)) }
  }

  /**
   * Return the guessed format for this datestring
   */
  def getFormat(s: String): Option[String] = getFormatObject(s).map(_.pattern)

  /**
   * The DateParser returned here is based on SimpleDateFormat, which is not thread-safe.
   * Do not share the result across threads.
   */
  def getDateParser(s: String): Option[DateParser] =
    getFormat(s).map { fmt => DateParser.from(new SimpleDateFormat(fmt)).contramap(prepare) }
}
