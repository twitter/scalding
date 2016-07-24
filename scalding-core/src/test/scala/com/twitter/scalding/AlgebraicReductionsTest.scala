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

class AlgebraJob(args: Args) extends Job(args) {
  Tsv("input", ('x, 'y, 'z, 'w))
    .map('w -> 'w) { w: Int => Set(w) }
    .groupBy('x) {
      _.sum[(Int, Int)](('y, 'z) -> ('sy, 'sz))
        .sum[Set[Int]]('w -> 'setw)
        .times[(Int, Int)](('y, 'z) -> ('py, 'pz))
        .dot[Int]('y, 'z, 'ydotz)
    }
    .write(Tsv("output"))
}

class ComplicatedAlgebraJob(args: Args) extends Job(args) {
  Tsv("input", ('x, 'y, 'z, 'w, 'v))
    .map('w -> 'w) { w: Int => Set(w) }
    .groupBy('x) {
      _.sum[(Int, Int, Set[Int], Double)](('y, 'z, 'w, 'v) -> ('sy, 'sz, 'sw, 'sv))
    }
    .write(Tsv("output"))
}

class AlgebraJobTest extends WordSpec with Matchers {
  import Dsl._
  val inputData = List((1, 2, 3, 5), (1, 4, 5, 7), (2, 1, 0, 7))
  val correctOutput = List((1, 6, 8, Set(5, 7), 8, 15, (6 + 20)), (2, 1, 0, Set(7), 1, 0, 0))
  "A AlgebraJob" should {
    JobTest(new AlgebraJob(_))
      .source(Tsv("input", ('x, 'y, 'z, 'w)), inputData)
      .sink[(Int, Int, Int, Set[Int], Int, Int, Int)](Tsv("output")) { buf =>
        "correctly do algebra" in {
          buf.toList shouldBe correctOutput
        }
      }
      .run
      .finish()
  }

  val inputData2 = List((1, 2, 3, 5, 1.2), (1, 4, 5, 7, 0.1), (2, 1, 0, 7, 3.2))
  val correctOutput2 = List((1, 6, 8, Set(5, 7), 1.3), (2, 1, 0, Set(7), 3.2))
  "A ComplicatedAlgebraJob" should {
    JobTest(new ComplicatedAlgebraJob(_))
      .source(Tsv("input", ('x, 'y, 'z, 'w, 'v)), inputData2)
      .sink[(Int, Int, Int, Set[Int], Double)](Tsv("output")) { buf =>
        "correctly do complex algebra" in {
          buf.toList shouldBe correctOutput2
        }
      }
      .run
      .finish()
  }
}
