/*
Copyright 2013 Twitter, Inc.

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

import com.twitter.scalding.typed.LookupJoin
import org.specs._

class LookupJoinerJob(args: Args) extends Job(args) {
  import TDsl._

  val in0 = TypedTsv[(Int, Int, Int)]("input0")
  val in1 = TypedTsv[(Int, Int, Int)]("input1")

  LookupJoin(TypedPipe.from(in0).map { case (t, k, v) => (t, (k, v)) },
    TypedPipe.from(in1).map { case (t, k, v) => (t, (k, v)) })
    .map {
      case (t, (k, (v, opt))) =>
        (t.toString, k.toString, v.toString, opt.toString)
    }
    .write(TypedTsv[(String, String, String, String)]("output"))
}

class LookupJoinedTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  def lookupJoin[T: Ordering, K, V, W](in0: Iterable[(T, K, V)], in1: Iterable[(T, K, W)]) = {
    // super inefficient, but easy to verify:
    def lookup(t: T, k: K): Option[W] = {
      implicit val ord = Ordering.by { tkw: (T, K, W) => tkw._1 }
      in1.filter { case (t1, k1, _) => (k1 == k) && Ordering[T].lt(t1, t) }
        .reduceOption(Ordering[(T, K, W)].max(_, _))
        .map { _._3 }
    }
    in0.map { case (t, k, v) => (t.toString, k.toString, v.toString, lookup(t, k).toString) }
  }
  "A LookupJoinerJob" should {
    "correctly lookup" in {
      val rng = new java.util.Random
      val MAX_KEY = 10
      def genList(sz: Int): List[(Int, Int, Int)] = {
        (0 until sz).map { _ =>
          (rng.nextInt, rng.nextInt(MAX_KEY), rng.nextInt)
        }.toList
      }
      val in0 = genList(1000)
      val in1 = genList(1000)
      JobTest(new LookupJoinerJob(_))
        .source(TypedTsv[(Int, Int, Int)]("input0"), in0)
        .source(TypedTsv[(Int, Int, Int)]("input1"), in1)
        .sink[(String, String, String, String)](
          TypedTsv[(String, String, String, String)]("output")) { outBuf =>
            outBuf.toSet must be_==(lookupJoin(in0, in1).toSet)
            in0.size must be_==(outBuf.size)
          }
        .run
        .runHadoop
        .finish
    }
  }
}
