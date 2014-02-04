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

import org.specs._
import com.twitter.scalding._

class OLSJob(args : Args) extends Job(args) {
  try {
    val src = Tsv("input", ('x, 'y))
    Regression.ols( src, 'x, 'y).write(Tsv("ols.txt"))
  } catch {
      case e : Exception => e.printStackTrace()
  }
}

class OLSJobTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  val randomalpha = (1 + math.random * 10).toInt
  val randombeta = (1 + math.random * 10).toInt
  val x = (-10.0 to 10.0 by 0.5).toList
  val y = x.map { i => randombeta * i + randomalpha }
  val xy = x.zip(y)

  "An OLS Job" should {
    JobTest(new OLSJob(_))
      .source(Tsv("input", ('x, 'y)), xy)
      .sink[(Int,Int)](Tsv("ols.txt")) { pbuf =>
        val tuple = pbuf.toList.head
        val (alpha, beta) = tuple
        "correctly compute alpha & beta" in {
          alpha must beCloseTo(randomalpha, 0.001)
          beta must beCloseTo(randombeta, 0.001)
        }
      }
      .run
      .finish
  }
}
