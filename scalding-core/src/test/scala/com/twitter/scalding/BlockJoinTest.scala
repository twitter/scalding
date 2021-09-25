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

import cascading.pipe.joiner._

import scala.collection.mutable.Buffer

class InnerProductJob(args: Args) extends Job(args) {
  val l = args.getOrElse("left", "1").toInt
  val r = args.getOrElse("right", "1").toInt
  val j = args.getOrElse("joiner", "i") match {
    case "i" => new InnerJoin
    case "l" => new LeftJoin
    case "r" => new RightJoin
    case "o" => new OuterJoin
  }

  val in0 = Tsv("input0").read.mapTo((0, 1, 2) -> ('x1, 'y1, 's1)) { input: (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0, 1, 2) -> ('x2, 'y2, 's2)) { input: (Int, Int, Int) => input }
  in0
    .blockJoinWithSmaller('y1 -> 'y2, in1, leftReplication = l, rightReplication = r, joiner = j)
    .map(('s1, 's2) -> 'score) { v: (Int, Int) =>
      v._1 * v._2
    }
    .groupBy('x1, 'x2) { _.sum[Double]('score) }
    .write(Tsv("output"))
}

class BlockJoinPipeTest extends WordSpec with Matchers {
  "An InnerProductJob" should {

    val in1 = List(("0", "0", "1"), ("0", "1", "1"), ("1", "0", "2"), ("2", "0", "4"))
    val in2 = List(("0", "1", "1"), ("1", "0", "2"), ("2", "4", "5"))
    val correctOutput = Set((0, 1, 2.0), (0, 0, 1.0), (1, 1, 4.0), (2, 1, 8.0))

    def runJobWithArguments(left: Int = 1, right: Int = 1, joiner: String = "i")(callback: Buffer[(Int, Int, Double)] => Unit): Unit = {
      JobTest(new InnerProductJob(_))
        .source(Tsv("input0"), in1)
        .source(Tsv("input1"), in2)
        .arg("left", left.toString)
        .arg("right", right.toString)
        .arg("joiner", joiner)
        .sink[(Int, Int, Double)](Tsv("output")) { outBuf =>
          callback(outBuf)
        }
        .run
        .finish()
    }

    "correctly compute product with 1 left block and 1 right block" in {
      runJobWithArguments() { outBuf =>
        outBuf.toSet shouldBe correctOutput
      }
    }

    "correctly compute product with multiple left and right blocks" in {
      runJobWithArguments(left = 3, right = 7) { outBuf =>
        outBuf.toSet shouldBe correctOutput
      }
    }

    "correctly compute product with a valid LeftJoin" in {
      runJobWithArguments(right = 7, joiner = "l") { outBuf =>
        outBuf.toSet shouldBe correctOutput
      }
    }

    "throw an exception when used with OuterJoin" in {
      an[InvalidJoinModeException] should be thrownBy runJobWithArguments(joiner = "o") { _ => }
    }

    "throw an exception when used with an invalid LeftJoin" in {
      an[InvalidJoinModeException] should be thrownBy runJobWithArguments(joiner = "l", left = 2) { _ => }
    }

    "throw an exception when used with an invalid RightJoin" in {
      an[InvalidJoinModeException] should be thrownBy runJobWithArguments(joiner = "r", right = 2) { _ => }
    }
  }
}
