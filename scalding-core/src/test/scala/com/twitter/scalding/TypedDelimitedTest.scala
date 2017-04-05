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

import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding.source.DailySuffixTypedTsv

class TypedTsvJob(args: Args) extends Job(args) {
  try {
    TypedTsv[(String, Int)]("input0").read.write(TypedTsv[(String, Int)]("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class TypedCsvJob(args: Args) extends Job(args) {
  try {
    TypedCsv[(String, Int)]("input0").read.write(TypedCsv[(String, Int)]("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class TypedPsvJob(args: Args) extends Job(args) {
  try {
    TypedPsv[(String, Int)]("input0").read.write(TypedPsv[(String, Int)]("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class TypedOsvJob(args: Args) extends Job(args) {
  try {
    TypedOsv[(String, Int)]("input0").read.write(TypedOsv[(String, Int)]("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

object DailySuffixTypedTsvJob {
  val strd1 = "2014-05-01"
  val strd2 = "2014-05-02"
  implicit val tz: java.util.TimeZone = DateOps.UTC
  implicit val parser: DateParser = DateParser.default
  implicit val dr1: DateRange = DateRange(RichDate(strd1), RichDate(strd2))

  def source(str: String) = DailySuffixTypedTsv[(String, Int)](str)

}

class DailySuffixTypedTsvJob(args: Args) extends Job(args) with UtcDateRangeJob {
  try {
    DailySuffixTypedTsvJob.source("input0").read.write(TypedTsv[(String, Int)]("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class TypedDelimitedTest extends WordSpec with Matchers {
  import Dsl._

  val data = List(("aaa", 1), ("bbb", 2))

  "A TypedTsv Source" should {
    JobTest(new TypedTsvJob(_))
      .source(TypedTsv[(String, Int)]("input0"), data)
      .typedSink(TypedTsv[(String, Int)]("output0")) { buf =>
        "read and write data" in {
          buf shouldBe data
        }
      }
      .run
      .finish()
  }

  "A TypedCsv Source" should {
    JobTest(new TypedCsvJob(_))
      .source(TypedCsv[(String, Int)]("input0"), data)
      .typedSink(TypedCsv[(String, Int)]("output0")) { buf =>
        "read and write data" in {
          buf shouldBe data
        }
      }
      .run
      .finish()
  }

  "A TypedPsv Source" should {
    JobTest(new TypedPsvJob(_))
      .source(TypedPsv[(String, Int)]("input0"), data)
      .typedSink(TypedPsv[(String, Int)]("output0")) { buf =>
        "read and write data" in {
          buf shouldBe data
        }
      }
      .run
      .finish()
  }

  "A TypedOsv Source" should {
    JobTest(new TypedOsvJob(_))
      .source(TypedOsv[(String, Int)]("input0"), data)
      .typedSink(TypedOsv[(String, Int)]("output0")) { buf =>
        "read and write data" in {
          buf shouldBe data
        }
      }
      .run
      .finish()
  }

  "A DailySuffixTypedTsv Source" should {
    import DailySuffixTypedTsvJob._
    JobTest(new DailySuffixTypedTsvJob(_))
      .arg("date", strd1 + " " + strd2)
      .source(source("input0"), data)
      .typedSink(TypedTsv[(String, Int)]("output0")) { buf =>
        "read and write data" in {
          buf shouldBe data
        }
      }
      .run
      .finish()
  }
}
