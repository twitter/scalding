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

import org.specs._

import cascading.pipe.Pipe
import cascading.tuple.Fields

class SourceSpec extends Specification {
  import Dsl._

  "A case class Source" should {
    "inherit equality properly from TimePathedSource" in {
      val d1 = RichDate("2012-02-01")(DateOps.UTC)
      val d2 = RichDate("2012-02-02")(DateOps.UTC)
      val d3 = RichDate("2012-02-03")(DateOps.UTC)
      val dr1 = DateRange(d1, d2)
      val dr2 = DateRange(d2, d3)

      val a = DailySuffixTsv("/test")(dr1)
      val b = DailySuffixTsv("/test")(dr2)
      val c = DailySuffixTsv("/testNew")(dr1)
      val d = DailySuffixTsvSecond("/test")(dr1)
      val e = DailySuffixTsv("/test")(dr1)

      (a == b) must beFalse
      (b == c) must beFalse
      (a == d) must beFalse
      (a == e) must beTrue
    }
  }

  "A Source with overriden transformForRead and transformForWrite" should {
    "respect these overrides even for tests" in {
      JobTest(new AddRemoveOneJob(_))
        .source(AddOneTsv("input"), List((0, "0"), (1, "1")))
        .sink[(String, String)](RemoveOneTsv("output")) { buf =>
          buf.toSet must_== Set(("0", "0"), ("1", "1"))
        }
        .run
        .finish
    }
  }
}

case class DailySuffixTsv(p : String)(dr : DateRange)
  extends TimePathedSource(p + TimePathedSource.YEAR_MONTH_DAY + "/*", dr, DateOps.UTC)

case class DailySuffixTsvSecond(p : String)(dr : DateRange)
  extends TimePathedSource(p + TimePathedSource.YEAR_MONTH_DAY + "/*", dr, DateOps.UTC)

case class AddOneTsv(p : String) extends FixedPathSource(p)
  with DelimitedScheme with Mappable[(Int, String, String)] {
  import Dsl._
  override val sourceFields = new Fields("one", "two", "three")
  override def converter[U >: (Int, String, String)] =
    TupleConverter.asSuperConverter[(Int, String, String), U](implicitly[TupleConverter[(Int, String, String)]])
  override def transformForRead(p: Pipe) = {
    p.mapTo((0, 1) -> ('one, 'two, 'three)) {
      t: (Int, String) => t :+ "1"
    }
  }
}

case class RemoveOneTsv(p : String) extends FixedPathSource(p)
  with DelimitedScheme with Mappable[(Int, String, String)] {
  import Dsl._
  override val sourceFields = new Fields("one", "two", "three")
  override def converter[U >: (Int, String, String)] =
    TupleConverter.asSuperConverter[(Int, String, String), U](implicitly[TupleConverter[(Int, String, String)]])
  override def transformForWrite(p: Pipe) = {
    p.mapTo(('one, 'two, 'three) -> (0, 1)) {
      t: (Int, String, String) => (t._1, t._2)
    }
  }
}

class AddRemoveOneJob(args: Args) extends Job(args) {
  AddOneTsv("input")
    .read

    //just for fun lets just switch all 1s with 2s
    .map('three -> 'three) { s: String => "2" }

    .write(RemoveOneTsv("output"))
}
