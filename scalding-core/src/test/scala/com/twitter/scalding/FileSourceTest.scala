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
import com.twitter.scalding._

class JsonLineJob(args : Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0"))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class JsonLineRestrictedFieldsJob(args : Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0", Tuple1('query)))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class JsonLineInputJob(args : Args) extends Job(args) {
  try {

    JsonLine("input0", ('foo, 'bar)).read
      .project('foo, 'bar)
      .write(Tsv("output0"))

  } catch {
    case e : Exception => e.printStackTrace
  }
}

class MultiTsvInputJob(args: Args) extends Job(args) {
  try {
    MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)).read.write(Tsv("output0"))
  } catch {
    case e : Exception => e.printStackTrace()
  }

}

class FileSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  "A JsonLine sink" should {
    JobTest("com.twitter.scalding.JsonLineJob")
      .source(Tsv("input0", ('query, 'queryStats)), List(("doctor's mask", List(42.1f, 17.1f))))
      .sink[String](JsonLine("output0")) { buf =>
        val json = buf.head
        "not stringify lists or numbers and not escape single quotes" in {
            json must be_==("""{"query":"doctor's mask","queryStats":[42.1,17.1]}""")
        }
      }
      .run
      .finish

    JobTest("com.twitter.scalding.JsonLineRestrictedFieldsJob")
      .source(Tsv("input0", ('query, 'queryStats)), List(("doctor's mask", List(42.1f, 17.1f))))
      .sink[String](JsonLine("output0", Tuple1('query))) { buf =>
        val json = buf.head
        "only sink requested fields" in {
            json must be_==("""{"query":"doctor's mask"}""")
        }
      }
      .run
      .finish

    val json = """{"foo": 3, "bar": "baz"}\n"""

    JobTest("com.twitter.scalding.JsonLineInputJob")
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf =>
          "read json line input" in {
            outBuf.toList must be_==(List((3, "baz")))
          }
      }
      .run
      .finish

    val json2 = """{"foo": 7 }\n"""

    JobTest("com.twitter.scalding.JsonLineInputJob")
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json), (1, json2)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf =>
          "handle missing fields" in {
            outBuf.toList must be_==(List((3, "baz"), (7, null)))
          }
      }
      .run
      .finish

  }

  "A MultipleTsvFile Source" should {
    JobTest("com.twitter.scalding.MultiTsvInputJob").
      source(MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)),
              List(("foobar", 1), ("helloworld", 2))).
      sink[(String, Int)](Tsv("output0")) {
        outBuf =>
          "take multiple Tsv files as input sources" in {
            outBuf.length must be_==(2)
            outBuf.toList must be_==(List(("foobar", 1), ("helloworld", 2)))
          }
      }
      .run
      .finish
  }

}
