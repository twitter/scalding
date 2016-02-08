/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.scalding.typed

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import scala.util.Try

import com.twitter.scalding.examples.KMeans

class ExecutionKMeansTest extends WordSpec with Matchers {

  "Execution K-means" should {
    "find the correct clusters for trivial cases" in {
      val dim = 20
      val k = 20
      val rng = new java.util.Random
      // if you are in cluster i, then position i == 100, else all the first k are 0.
      // Then all the tail are random, but very small enough to never bridge the gap
      def randVect(cluster: Int): Vector[Double] =
        Vector.fill(k)(0.0).updated(cluster, 100.0) ++ Vector.fill(dim - k)(rng.nextDouble / (1e6 * dim))

      // To have the seeds stay sane for kmeans k == vectorCount
      val vectorCount = k
      val vectors = TypedPipe.from((0 until vectorCount).map { i => randVect(i % k) })

      val labels = KMeans(k, vectors).flatMap {
        case (_, _, labeledPipe) =>
          labeledPipe.toIterableExecution
      }
        .waitFor(Config.default, Local(false)).get.toList

      def clusterOf(v: Vector[Double]): Int = v.indexWhere(_ > 0.0)

      val byCluster = labels.groupBy { case (id, v) => clusterOf(v) }

      // The rule is this: if two vectors share the same prefix,
      // the should be in the same cluster
      byCluster.foreach {
        case (clusterId, vs) =>
          val id = vs.head._1
          vs.foreach { case (thisId, _) => id shouldBe thisId }
      }
    }
  }
}
