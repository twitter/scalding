/*
Copyright 2016 Twitter, Inc.

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

import org.scalatest.{ Matchers, WordSpec }

class TimePathedSourceTest extends WordSpec with Matchers {
  "TimePathedSource.hdfsWritePath" should {
    val dateRange = DateRange(RichDate(0L), RichDate(0L))
    val utcTZ = DateOps.UTC

    "crib if path == /*" in {
      intercept[AssertionError] { TestTimePathedSource("/*", dateRange, utcTZ).hdfsWritePath }
    }

    "crib if path doesn't end with /*" in {
      intercept[AssertionError] { TestTimePathedSource("/my/invalid/path", dateRange, utcTZ).hdfsWritePath }
    }

    "work for path ending with /*" in {
      TestTimePathedSource("/my/path/*", dateRange, utcTZ).hdfsWritePath startsWith "/my/path"
    }
  }
}

case class TestTimePathedSource(p: String, dr: DateRange, t: TimeZone) extends TimePathedSource(p, dr, t)
