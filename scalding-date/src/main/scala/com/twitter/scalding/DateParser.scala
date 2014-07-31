
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

import scala.util.{ Try, Failure }
import java.util.TimeZone
import java.text.DateFormat

trait DateParser extends java.io.Serializable { self =>
  def parse(s: String)(implicit tz: TimeZone): Try[RichDate]

  // Map the input before parsing (name from functional programming: contravariant map)
  def contramap(fn: String => String): DateParser = new DateParser {
    def parse(s: String)(implicit tz: TimeZone): Try[RichDate] = self.parse(fn(s))
  }

  def rescueWith(second: DateParser): DateParser =
    new DateParser {
      def parse(s: String)(implicit tz: TimeZone) = {
        self.parse(s) orElse second.parse(s)
      }
    }
}

object DateParser {
  /**
   * This is scalding's default date parser. You can choose this
   * by setting an implicit val DateParser.
   */
  val default: DateParser = new DateParser {
    def parse(s: String)(implicit tz: TimeZone) =
      DateOps.getDateParser(s)
        .map { p => p.parse(s) }
        .getOrElse(Failure(new IllegalArgumentException("Could not find parser for: " + s)))
  }

  /** Try these Parsers in order */
  def apply(items: Iterable[DateParser]): DateParser =
    items.reduce { _.rescueWith(_) }

  /** Using the type-class pattern */
  def parse(s: String)(implicit tz: TimeZone, p: DateParser): Try[RichDate] = p.parse(s)(tz)

  implicit def from(df: DateFormat): DateParser = new DateParser {
    def parse(s: String)(implicit tz: TimeZone) = Try {
      df.setTimeZone(tz)
      RichDate(df.parse(s))
    }
  }

  def from(fn: String => RichDate) = new DateParser {
    def parse(s: String)(implicit tz: TimeZone) = Try(fn(s))
  }
  def from(fn: (String, TimeZone) => RichDate) = new DateParser {
    def parse(s: String)(implicit tz: TimeZone) = Try(fn(s, tz))
  }
}

/**
 * //Scalding used to support Natty, this is removed. To add it back, use something like this in your code,
 * //possibly with:
 * //implicit val myParser = DateParser(Seq(DateParser.default, NattyParser))
 *
 * object NattyParser extends DateParser {
 * def parse(s: String)(implicit tz: TimeZone) = Try {
 * val timeParser = new natty.Parser(tz)
 * val dateGroups = timeParser.parse(s)
 * if (dateGroups.size == 0) {
 * throw new IllegalArgumentException("Could not convert string: '" + str + "' into a date.")
 * }
 * // a DateGroup can have more than one Date (e.g. if you do "Sept. 11th or 12th"),
 * // but we're just going to take the first
 * val dates = dateGroups.get(0).getDates()
 * RichDate(dates.get(0))
 * }
 * }
 *
 */
