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
package com.twitter.scalding.mathematics

import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding._

class HistogramJob(args: Args) extends Job(args) {
  try {
    val hist = Tsv("input", 'n)
      .groupAll{ _.histogram('n -> 'hist) }

    hist
      .flatMapTo('hist -> ('bin, 'cdf)){ h: Histogram => h.cdf }
      .write(Tsv("cdf-output"))

    hist
      .mapTo('hist -> ('min, 'max, 'sum, 'mean, 'stdDev)){ h: Histogram => (h.min, h.max, h.sum, h.mean, h.stdDev) }
      .write(Tsv("stats-output"))

  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class HistogramJobTest extends WordSpec with Matchers {
  import Dsl._
  val values = List(1.0, 2.5, 1.5, 3.0, 3.0, 3.0, 4.2, 2.0, 8.0, 1.0)
  val inputData = values.map(Tuple1(_))
  val cdfOutput = Set((1.0, 0.3), (2.0, 0.5), (3.0, 0.8), (4.0, 0.9), (8.0, 1.0))
  "A HistogramJob" should {
    JobTest(new HistogramJob(_))
      .source(Tsv("input", ('n)), inputData)
      .sink[(Double, Double, Double, Double, Double)](Tsv("stats-output")) { buf =>
        val (min, max, sum, mean, stdDev) = buf.head
        "correctly compute the min" in {
          min shouldBe (values.map(_.floor).min)
        }
        "correctly compute the max" in {
          max shouldBe (values.map(_.floor).max)
        }
        "correctly compute the sum" in {
          sum shouldBe (values.map(_.floor).sum)
        }
        "correctly compute the mean" in {
          mean shouldBe (values.map(_.floor).sum / values.size)
        }
        "correctly compute the stdDev" in {
          stdDev shouldBe 1.989974874 +- 0.000000001
        }
      }
      .sink[(Double, Double)](Tsv("cdf-output")) { buf =>
        "correctly compute a CDF" in {
          buf.toSet shouldBe cdfOutput
        }
      }
      .run
      .finish()
  }
}
