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

  sealed abstract class Format(val pattern: String, val validator: Regex)

  private val DATE_RE = """\d{4}-\d{2}-\d{2}"""
  private val SEP_RE = """(T?|\s*)"""

  case object DATE_WITH_DASH extends Format("yyyy-MM-dd", new Regex("""^\s*""" + DATE_RE + """\s*$"""))
  case object DATEHOUR_WITH_DASH extends Format("yyyy-MM-dd HH", new Regex("""^\s*""" + DATE_RE + SEP_RE + """\d\d\s*$"""))
  case object DATETIME_WITH_DASH extends Format("yyyy-MM-dd HH:mm", new Regex("""^\s*""" + DATE_RE + SEP_RE + """\d\d:\d\d\s*$"""))
  case object DATETIME_HMS_WITH_DASH extends Format("yyyy-MM-dd HH:mm:ss", new Regex("""^\s*""" + DATE_RE + SEP_RE + """\d\d:\d\d:\d\d\s*$"""))
  case object DATETIME_HMSM_WITH_DASH extends Format("yyyy-MM-dd HH:mm:ss.SSS", new Regex("""^\s*""" + DATE_RE + SEP_RE + """\d\d:\d\d:\d\d\.\d{1,3}\s*$"""))

  val formats = List(DATE_WITH_DASH, DATEHOUR_WITH_DASH, DATETIME_WITH_DASH, DATETIME_HMS_WITH_DASH, DATETIME_HMSM_WITH_DASH)

  private val prepare: String => String = { (str: String) =>
    str.replace("T", " ") //We allow T to separate dates and times, just remove it and then validate
      .replaceAll("[/_]", "-") // Allow for slashes and underscores
  }
  /**
   * Return the guessed format for this datestring
   */
  def getFormat(s: String): Option[Format] =
    formats.find { _.validator.findFirstIn(prepare(s)).isDefined }

  /**
   * The DateParser returned here is based on SimpleDateFormat, which is not thread-safe.
   * Do not share the result across threads.
   */
  def getDateParser(s: String): Option[DateParser] =
    getFormat(s).map { fmt =>
      DateParser.from(new SimpleDateFormat(fmt.pattern)).contramap(prepare)
    }
}
