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

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.source._

class SourceSpec extends WordSpec with Matchers {
  import Dsl._

  "A case class Source" should {
    "inherit equality properly from TimePathedSource" in {
      implicit val tz: java.util.TimeZone = DateOps.UTC
      implicit val parser: DateParser = DateParser.default

      val d1 = RichDate("2012-02-01")
      val d2 = RichDate("2012-02-02")
      val d3 = RichDate("2012-02-03")
      val dr1 = DateRange(d1, d2)
      val dr2 = DateRange(d2, d3)

      val a = DailySuffixTsv("/test")(dr1)
      val b = DailySuffixTsv("/test")(dr2)
      val c = DailySuffixTsv("/testNew")(dr1)
      val d = new DailySuffixTsvSecond("/testNew")(dr1)
      val e = DailySuffixTsv("/test")(dr1)

      a should not be b
      b should not be c
      a should not be d
      a shouldBe e
    }
  }

  class DailySuffixTsvSecond(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
    extends DailySuffixSource(prefix, dateRange) with DelimitedScheme {
    override val fields = fs
  }

  "A Source with overriden transformForRead and transformForWrite" should {
    "respect these overrides even for tests" in {
      JobTest(new AddRemoveOneJob(_))
        .source(AddOneTsv("input"), List((0, "0"), (1, "1")))
        .sink[(String, String)](RemoveOneTsv("output")) { buf =>
          buf.toSet shouldBe Set(("0", "0"), ("1", "1"))
        }
        .run
        .finish()
    }
  }
}

case class AddOneTsv(p: String) extends FixedPathSource(p)
  with DelimitedScheme with Mappable[(Int, String, String)] {
  import Dsl._
  import TDsl._
  override val transformInTest = true
  override val sourceFields = new Fields("one", "two", "three")
  override def converter[U >: (Int, String, String)] =
    TupleConverter.asSuperConverter[(Int, String, String), U](implicitly[TupleConverter[(Int, String, String)]])
  override def transformForRead(p: Pipe) = {
    p.mapTo((0, 1) -> ('one, 'two, 'three)) {
      t: (Int, String) => t :+ "1"
    }
  }
}

case class RemoveOneTsv(p: String) extends FixedPathSource(p)
  with DelimitedScheme with Mappable[(Int, String, String)] {
  override val transformInTest = true
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

class MapTypedPipe(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[(Int, String)]("input"))
    .map(MapFunctionAndThenTest.mapFunction)
    .write(TypedText.tsv[(Int, String, Int)]("output"))
}

class IdentityTypedPipe(args: Args) extends Job(args) {
  TypedPipe.from(
    TypedText.tsv[(Int, String)]("input")
      .andThen(MapFunctionAndThenTest.mapFunction))
    .write(TypedText.tsv[(Int, String, Int)]("output"))
}

object MapFunctionAndThenTest {
  def mapFunction(input: (Int, String)): (Int, String, Int) =
    (input._1, input._2, input._1)

  val input: List[(Int, String)] = List((0, "a"), (1, "b"), (2, "c"))
  val output: List[(Int, String, Int)] = List((0, "a", 0), (1, "b", 1), (2, "c", 2))
}
class TypedPipeAndThenTest extends WordSpec with Matchers {
  import Dsl._
  import MapFunctionAndThenTest._
  "Mappable.andThen is like TypedPipe.map" should {
    JobTest(new MapTypedPipe(_))
      .source(TypedText.tsv[(Int, String)]("input"), input)
      .typedSink(TypedText.tsv[(Int, String, Int)]("output")){ outputBuffer =>
        val outMap = outputBuffer.toList
        "TypedPipe return proper results" in {
          outMap should have size 3
          outMap shouldBe output
        }
      }
      .run
      .finish()

    JobTest(new IdentityTypedPipe(_))
      .source(TypedText.tsv[(Int, String)]("input"), input)
      .typedSink(TypedText.tsv[(Int, String, Int)]("output")){ outputBuffer =>
        val outMap = outputBuffer.toList
        "Mappable.andThen return proper results" in {
          outMap should have size 3
          outMap shouldBe output
        }
      }
      .run
      .finish()

  }
}
