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

import scala.collection.mutable.Buffer

class SkewJoinJob(args: Args) extends Job(args) {
  val sampleRate = args.getOrElse("sampleRate", "0.001").toDouble
  val reducers = args.getOrElse("reducers", "-1").toInt
  val replicationFactor = args.getOrElse("replicationFactor", "1").toInt
  val replicator = if (args.getOrElse("replicator", "a") == "a")
    SkewReplicationA(replicationFactor)
  else
    SkewReplicationB()

  val in0 = Tsv("input0").read.mapTo((0, 1, 2) -> ('x1, 'y1, 's1)) { input: (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0, 1, 2) -> ('x2, 'y2, 's2)) { input: (Int, Int, Int) => input }

  in0
    .skewJoinWithSmaller('y1 -> 'y2, in1, sampleRate, reducers, replicator)
    .project('x1, 'y1, 's1, 'x2, 'y2, 's2)
    .write(Tsv("output"))
  // Normal inner join:
  in0
    .joinWithSmaller('y1 -> 'y2, in1)
    .project('x1, 'y1, 's1, 'x2, 'y2, 's2)
    .write(Tsv("jws-output"))
}

object JoinTestHelper {
  import Dsl._

  val rng = new java.util.Random
  def generateInput(size: Int, max: Int): List[(String, String, String)] = {
    def next: String = rng.nextInt(max).toString
    (0 to size).map { i => (next, next, next) }.toList
  }

  type JoinResult = (Int, Int, Int, Int, Int, Int)

  def runJobWithArguments(fn: (Args) => Job, sampleRate: Double = 0.001, reducers: Int = -1,
    replicationFactor: Int = 1, replicator: String = "a"): (List[JoinResult], List[JoinResult]) = {

    val skewResult = Buffer[JoinResult]()
    val innerResult = Buffer[JoinResult]()
    JobTest(fn)
      .arg("sampleRate", sampleRate.toString)
      .arg("reducers", reducers.toString)
      .arg("replicationFactor", replicationFactor.toString)
      .arg("replicator", replicator)
      .source(Tsv("input0"), generateInput(1000, 100))
      .source(Tsv("input1"), generateInput(100, 100))
      .sink[(Int, Int, Int, Int, Int, Int)](Tsv("output")) { outBuf => skewResult ++= outBuf }
      .sink[(Int, Int, Int, Int, Int, Int)](Tsv("jws-output")) { outBuf => innerResult ++= outBuf }
      .run
      //.runHadoop //this takes MUCH longer to run. Commented out by default, but tests pass on my machine
      .finish()
    (skewResult.toList.sorted, innerResult.toList.sorted)
  }
}

class SkewJoinPipeTest extends WordSpec with Matchers {
  import JoinTestHelper._

  "A SkewInnerProductJob" should {
    "compute skew join with sampleRate = 0.001, using strategy A" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), sampleRate = 0.001, replicator = "a")
      sk shouldBe inner
    }

    "compute skew join with sampleRate = 0.001, using strategy B" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), sampleRate = 0.001, replicator = "b")
      sk shouldBe inner
    }

    "compute skew join with sampleRate = 0.1, using strategy A" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), sampleRate = 0.1, replicator = "a")
      sk shouldBe inner
    }

    "compute skew join with sampleRate = 0.1, using strategy B" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), sampleRate = 0.1, replicator = "b")
      sk shouldBe inner
    }

    "compute skew join with sampleRate = 0.9, using strategy A" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), sampleRate = 0.9, replicator = "a")
      sk shouldBe inner
    }

    "compute skew join with sampleRate = 0.9, using strategy B" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), sampleRate = 0.9, replicator = "b")
      sk shouldBe inner
    }

    "compute skew join with replication factor 5, using strategy A" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), replicationFactor = 5, replicator = "a")
      sk shouldBe inner
    }

    "compute skew join with reducers = 10, using strategy A" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), reducers = 10, replicator = "a")
      sk shouldBe inner
    }

    "compute skew join with reducers = 10, using strategy B" in {
      val (sk, inner) = runJobWithArguments(new SkewJoinJob(_), reducers = 10, replicator = "b")
      sk shouldBe inner
    }
  }
}

class CollidingKeySkewJoinJob(args: Args) extends Job(args) {
  val sampleRate = args.getOrElse("sampleRate", "0.001").toDouble
  val reducers = args.getOrElse("reducers", "-1").toInt
  val replicationFactor = args.getOrElse("replicationFactor", "1").toInt
  val replicator = if (args.getOrElse("replicator", "a") == "a")
    SkewReplicationA(replicationFactor)
  else
    SkewReplicationB()

  val in0 = Tsv("input0").read.mapTo((0, 1, 2) -> ('k1, 'k3, 'v1)) { input: (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0, 1, 2) -> ('k2, 'k3, 'v2)) { input: (Int, Int, Int) => input }

  in0
    .skewJoinWithSmaller('k3 -> 'k3, in1, sampleRate, reducers, replicator)
    .project('k1, 'k3, 'v1, 'k2, 'v2)
    .insert('z, 0) // Make it have the same schema as the non-colliding job
    .write(Tsv("output"))
  // Normal inner join:
  in0
    .joinWithSmaller('k3 -> 'k3, in1)
    .project('k1, 'k3, 'v1, 'k2, 'v2)
    .insert('z, 0) // Make it have the same schema as the non-colliding job
    .write(Tsv("jws-output"))
}

class CollidingKeySkewJoinTest extends WordSpec with Matchers {
  import JoinTestHelper._

  "A CollidingSkewInnerProductJob" should {
    "compute skew join with colliding fields, using strategy A" in {
      val (sk, inn) = runJobWithArguments(new CollidingKeySkewJoinJob(_), replicator = "a")
      sk shouldBe inn
    }

    "compute skew join with colliding fields, using strategy B" in {
      val (sk, inn) = runJobWithArguments(new CollidingKeySkewJoinJob(_), replicator = "b")
      sk shouldBe inn
    }
  }
}
