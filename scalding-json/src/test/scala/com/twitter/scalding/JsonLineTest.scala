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

package com.twitter.scalding.json

import cascading.flow.FlowException
import cascading.tap.SinkMode
import cascading.tuple.Fields
import com.twitter.scalding.{ JsonLine => StandardJsonLine, _ }
import org.scalatest.WordSpec

object JsonLine {
  def apply(p: String, fields: Fields = Fields.ALL, failOnEmptyLines: Boolean = true) =
    new JsonLine(p, fields, failOnEmptyLines)
}

class JsonLine(p: String, fields: Fields, failOnEmptyLines: Boolean) extends StandardJsonLine(p, fields, SinkMode.REPLACE,
  // We want to test the actual transformation here.
  transformInTest = true, failOnEmptyLines = failOnEmptyLines)

class JsonLineJob(args: Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class JsonLineRestrictedFieldsJob(args: Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0", Tuple1('query)))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class JsonLineInputJob(args: Args) extends Job(args) {
  try {
    JsonLine("input0", ('foo, 'bar)).read
      .project('foo, 'bar)
      .write(Tsv("output0"))

  } catch {
    case e: Exception => e.printStackTrace
  }
}

class JsonLineInputJobSkipEmptyLines(args: Args) extends Job(args) {
  try {
    JsonLine("input0", ('foo, 'bar), failOnEmptyLines = false).read
      .project('foo, 'bar)
      .write(Tsv("output0"))

  } catch {
    case e: Exception => e.printStackTrace
  }
}

class JsonLineNestedInputJob(args: Args) extends Job(args) {
  try {
    JsonLine("input0", (Symbol("foo.too"), 'bar)).read
      .rename((Symbol("foo.too") -> ('foo)))
      .project('foo, 'bar)
      .write(Tsv("output0"))

  } catch {
    case e: Exception => e.printStackTrace
  }
}

class JsonLineTest extends WordSpec {
  import com.twitter.scalding.Dsl._

  "A JsonLine sink" should {
    JobTest(new JsonLineJob(_))
      .source(Tsv("input0", ('query, 'queryStats)), List(("doctor's mask", List(42.1f, 17.1f))))
      .sink[String](JsonLine("output0")) { buf =>
        val json = buf.head
        "not stringify lists or numbers and not escape single quotes" in {
          assert(json === """{"query":"doctor's mask","queryStats":[42.1,17.1]}""")
        }
      }
      .run
      .finish()

    JobTest(new JsonLineRestrictedFieldsJob(_))
      .source(Tsv("input0", ('query, 'queryStats)), List(("doctor's mask", List(42.1f, 17.1f))))
      .sink[String](JsonLine("output0", Tuple1('query))) { buf =>
        val json = buf.head
        "only sink requested fields" in {
          assert(json === """{"query":"doctor's mask"}""")
        }
      }
      .run
      .finish()

    val json = """{"foo": 3, "bar": "baz"}\n"""

    JobTest(new JsonLineInputJob(_))
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf =>
          "read json line input" in {
            assert(outBuf.toList === List((3, "baz")))
          }
      }
      .run
      .finish()

    val json2 = """{"foo": 7 }\n"""

    JobTest(new JsonLineInputJob(_))
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json), (1, json2)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf =>
          "handle missing fields" in {
            assert(outBuf.toList === List((3, "baz"), (7, null)))
          }
      }
      .run
      .finish()

    val json3 = """{"foo": {"too": 9}}\n"""

    JobTest(new JsonLineNestedInputJob(_))
      .source(JsonLine("input0", (Symbol("foo.too"), 'bar)), List((0, json), (1, json3)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf =>
          "handle nested fields" in {
            assert(outBuf.toList === List((0, "baz"), (9, null)))
          }
      }
      .run
      .finish()

    "fail on empty lines by default" in {
      intercept[FlowException] {
        JobTest(new JsonLineInputJob(_))
          .source(JsonLine("input0", ('foo, 'bar)), List((0, json), (1, json2), (2, ""), (3, "   ")))
          .sink[(Int, String)](Tsv("output0")) {
            outBuf => outBuf.toList

          }
          .run
          .finish()
      }
    }

    JobTest(new JsonLineInputJobSkipEmptyLines(_))
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json), (1, json2), (2, ""), (3, "   ")))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf =>
          "handle empty lines when `failOnEmptyLines` is set to false" in {
            assert(outBuf.toList.size === 2)
          }
      }
      .run
      .finish()
  }
}
