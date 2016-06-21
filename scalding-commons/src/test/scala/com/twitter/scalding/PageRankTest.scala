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

class PageRankTest extends WordSpec with Matchers {
  "A PageRank job" should {
    JobTest(new com.twitter.scalding.examples.PageRank(_))
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("errorOut", "error")
      .arg("temp", "tempBuffer")
      //How many iterations to do each time:
      .arg("iterations", "6")
      .arg("convergence", "0.05")
      .source(Tsv("inputFile"), List((1L, "2", 1.0), (2L, "1,3", 1.0), (3L, "2", 1.0)))
      //Don't check the tempBuffer:
      .sink[(Long, String, Double)](Tsv("tempBuffer")) { ob => () }
      .sink[Double](TypedTsv[Double]("error")) { ob =>
        "have low error" in {
          ob.head should be <= 0.05
        }
      }
      .sink[(Long, String, Double)](Tsv("outputFile")){ outputBuffer =>
        val pageRank = outputBuffer.map { res => (res._1, res._3) }.toMap
        "correctly compute pagerank" in {
          val d = 0.85
          val twoPR = (1.0 + 2 * d) / (1.0 + d)
          val otherPR = (1.0 + d / 2.0) / (1.0 + d)
          println(pageRank)
          (pageRank(1L) + pageRank(2L) + pageRank(3L)) shouldBe 3.0 +- 0.1
          pageRank(1L) shouldBe otherPR +- 0.1
          pageRank(2L) shouldBe twoPR +- 0.1
          pageRank(3L) shouldBe otherPR +- 0.1
        }
      }
      .run
      .finish()
  }
}
