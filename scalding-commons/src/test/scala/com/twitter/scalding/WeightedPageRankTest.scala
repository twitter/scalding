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

class WeightedPageRankSpec extends WordSpec with Matchers {
  "Weighted PageRank job" should {
    var idx = 0
    JobTest(new com.twitter.scalding.examples.WeightedPageRank(_))
      .arg("pwd", ".")
      .arg("weighted", "true")
      .arg("maxiterations", "1")
      .arg("jumpprob", "0.1")
      .source(Tsv("./nodes"), List((1, "2,3", "1,2", 0.26), (2, "3", "1", 0.54), (3, "", "", 0.2)))
      .source(Tsv("./numnodes"), List((3)))
      .source(Tsv("./pagerank_0"), List((1, 0.086), (2, 0.192), (3, 0.722)))
      .typedSink(TypedTsv[Double]("./totaldiff")) { ob =>
        (idx + ": have low error") in {
          ob.head shouldBe (0.722 - 0.461 + 0.2964 - 0.192 + 0.2426 - 0.086) +- 0.001
        }
        idx += 1
      }
      .sink[(Int, Double)](Tsv("./pagerank_1")){ outputBuffer =>
        val pageRank = outputBuffer.map { res => (res._1, res._2) }.toMap
        (idx + ": correctly compute pagerank") in {
          val deadMass = 0.722 / 3 * 0.9
          val userMass = List(0.26, 0.54, 0.2).map { _ * 0.1 }
          val massNext = List(0, 0.086 / 3, (0.086 * 2 / 3 + 0.192)).map { _ * 0.9 }
          val expected = (userMass zip massNext) map { a: (Double, Double) => a._1 + a._2 + deadMass }

          println(pageRank)
          (pageRank(1) + pageRank(2) + pageRank(3)) shouldBe 1.0 +- 0.001
          pageRank(1) shouldBe (expected(0)) +- 0.001
          pageRank(2) shouldBe (expected(1)) +- 0.001
          pageRank(3) shouldBe (expected(2)) +- 0.001
        }
        idx += 1
      }
      .runWithoutNext(useHadoop = false)
      .runWithoutNext(useHadoop = true)
      .finish()
  }
}
