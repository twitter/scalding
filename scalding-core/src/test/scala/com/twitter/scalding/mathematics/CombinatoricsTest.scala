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

class CombinatoricsJob(args: Args) extends Job(args) {
  val C = Combinatorics
  C.permutations(10, 3).write(Tsv("perms.txt"))

  C.combinations(5, 2).write(Tsv("combs.txt"))

  // how many ways can you invest $10000 in KR,ABT,DLTR,MNST ?
  val cash = 1000.0
  val error = 1.0 // max error $1, so its ok if we cannot invest the last dollar
  val (kr, abt, dltr, mnst) = (27.0, 64.0, 41.0, 52.0) // share prices
  val stocks = IndexedSeq(kr, abt, dltr, mnst)

  C.weightedSum(stocks, cash, error).write(Tsv("invest.txt"))
  C.positiveWeightedSum(stocks, cash, error).write(Tsv("investpos.txt"))

}

class CombinatoricsJobTest extends WordSpec with Matchers {
  import Dsl._

  "A Combinatorics Job" should {
    JobTest(new CombinatoricsJob(_))
      .sink[(Int, Int)](Tsv("perms.txt")) { pbuf =>
        "correctly compute 10 permute 3 equals 720" in {
          pbuf.toList should have size 720
        }
      }
      .sink[(Int, Int)](Tsv("combs.txt")) { buf =>
        val csize = buf.toList.size
        "correctly compute 5 choose 2 equals 10" in {
          buf.toList should have size 10
        }
      }
      .sink[(Int, Int, Int, Int)](Tsv("invest.txt")) { buf =>
        "correctly compute 169 tuples that allow you to invest $1000 among the 4 given stocks" in {
          buf.toList should have size 169
        }
      }
      .sink[(Int, Int, Int, Int)](Tsv("investpos.txt")) { buf =>
        "correctly compute 101 non-zero tuples that allow you to invest $1000 among the 4 given stocks" in {
          buf.toList should have size 101
        }
      }
      .run
      .finish()
  }
}
