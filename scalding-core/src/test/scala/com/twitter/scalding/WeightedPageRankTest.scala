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

class WeightedPageRankSpec extends Specification {
  "Weighted PageRank job" should {
    JobTest("com.twitter.scalding.examples.WeightedPageRank").
      arg("pwd", ".").
      arg("weighted", "true").
      arg("maxiterations", "1").
      arg("jumpprob","0.1").
      source(Tsv("./nodes"), List((1,"2,3","1,2",0.26),(2,"3","1",0.54),(3,"","",0.2))).
      source(Tsv("./numnodes"), List((3))).
      source(Tsv("./pagerank_0"), List((1,0.086),(2,0.192),(3,0.722))).
      sink[Double](Tsv("./totaldiff")) { ob =>
        "have low error" in {
          ob.head must beCloseTo(0.722-0.461+0.2964-0.192+0.2426-0.086, 0.001)
        }
      }.
      sink[(Int,Double)](Tsv("./pagerank_1")){ outputBuffer =>
        val pageRank = outputBuffer.map { res => (res._1,res._2) }.toMap
        "correctly compute pagerank" in {
          val deadMass = 0.722/3*0.9
          val userMass = List(0.26, 0.54, 0.2).map { _*0.1 }
          val massNext = List(0, 0.086/3, (0.086*2/3+0.192)).map { _*0.9 }
          val expected = (userMass zip massNext) map { a : (Double, Double) => a._1 + a._2 + deadMass }

          println(pageRank)
          (pageRank(1) + pageRank(2) + pageRank(3)) must beCloseTo(1.0, 0.001)
          pageRank(1) must beCloseTo(expected(0), 0.001)
          pageRank(2) must beCloseTo(expected(1), 0.001)
          pageRank(3) must beCloseTo(expected(2), 0.001)
        }
      }.
      runWithoutNext(useHadoop=false).
      runWithoutNext(useHadoop=true).
      finish
  }
}
