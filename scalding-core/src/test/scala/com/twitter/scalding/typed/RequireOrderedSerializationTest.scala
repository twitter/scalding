/*
Copyright 2015 Twitter, Inc.

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

import com.twitter.scalding.serialization.CascadingBinaryComparator
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.StringOrderedSerialization

import org.scalatest.{ Matchers, WordSpec }

class NoOrderdSerJob(args: Args) extends Job(args) {

  override def config = super.config + (Config.ScaldingRequireOrderedSerialization -> "true")

  TypedPipe.from(TypedTsv[(String, String)]("input"))
    .group
    .max
    .write(TypedTsv[(String, String)]("output"))

  // This should fail
  if (args.boolean("check")) {
    CascadingBinaryComparator.checkForOrderedSerialization(flowDef).get
  }
}

class OrderdSerJob(args: Args) extends Job(args) {

  implicit def stringOS: OrderedSerialization[String] = new StringOrderedSerialization

  override def config = super.config + (Config.ScaldingRequireOrderedSerialization -> "true")

  TypedPipe.from(TypedTsv[(String, String)]("input"))
    .group
    .max
    .write(TypedTsv[(String, String)]("output"))

  // This should not fail
  if (args.boolean("check")) {
    CascadingBinaryComparator.checkForOrderedSerialization(flowDef).get
  }
}

class RequireOrderedSerializationTest extends WordSpec with Matchers {
  "A NoOrderedSerJob" should {
    // This should throw
    "throw with --check" in {
      an[Exception] should be thrownBy {
        (new NoOrderdSerJob(Mode.putMode(Local(true), Args("--check"))))
      }
    }
    "not throw without --check" in {
      (new NoOrderdSerJob(Mode.putMode(Local(true), Args(""))))
    }
    // throw if we try to run in:
    "throw when run" in {
      an[Exception] should be thrownBy {
        JobTest(new NoOrderdSerJob(_))
          .source(TypedTsv[(String, String)]("input"), List(("a", "a"), ("b", "b")))
          .sink[(String, String)](TypedTsv[(String, String)]("output")) { outBuf => () }
          .run
          .finish
      }
    }
  }
  "A OrderedSerJob" should {
    "not throw with --check" in {
      // This should not throw
      val osj = (new OrderdSerJob(Mode.putMode(Local(true), Args("--check"))))
    }
    // throw if we try to run in:
    "run" in {
      JobTest(new OrderdSerJob(_))
        .source(TypedTsv[(String, String)]("input"), List(("a", "a"), ("a", "b"), ("b", "b")))
        .sink[(String, String)](TypedTsv[(String, String)]("output")) { outBuf =>
          outBuf.toSet shouldBe Set(("a", "b"), ("b", "b"))
        }
        .run
        .finish
    }
  }
}
