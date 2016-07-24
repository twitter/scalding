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

import com.twitter.scalding._
import com.twitter.algebird.MapAlgebra.dot
import com.twitter.algebird.Group

import TDsl._

import org.scalatest.{ Matchers, WordSpec }

import GraphOperations._

class TypedCosineSimJob(args: Args) extends Job(args) {

  //val simOf = new ExactInCosine[Int]()
  val simOf = new DiscoInCosine[Int](0.001, 0.1, 0.01)
  val graph = withInDegree {
    TypedTsv[(Int, Int)]("ingraph")
      .map { case (from, to) => Edge(from, to, ()) }
  }
    // Just keep the degree
    .map { edge => edge.mapData { _._2 } }

  simOf(graph, { n: Int => n % 2 == 0 }, { n: Int => n % 2 == 1 })
    .map { edge => (edge.from, edge.to, edge.data) }
    .write(TypedTsv[(Int, Int, Double)]("out"))
}

class TypedDimsumCosineSimJob(args: Args) extends Job(args) {

  val simOf = new DimsumInCosine[Int](0.001, 0.1, 0.01)
  val graph = withInNorm {
    TypedTsv[(Int, Int, Double)]("ingraph")
      .map { case (from, to, weight) => Edge(from, to, Weight(weight)) }
  }

  simOf(graph, { n: Int => n % 2 == 0 }, { n: Int => n % 2 == 1 })
    .map { edge => (edge.from, edge.to, edge.data) }
    .toPipe('from, 'to, 'data)
    .write(TypedTsv[(Int, Int, Double)]("out"))
}

class TypedSimilarityTest extends WordSpec with Matchers {
  val nodes = 50
  val rand = new java.util.Random(1)
  val edges = (0 to nodes).flatMap { n =>
    // try to get at least 6 edges for each node
    (0 to ((nodes / 5) max (6))).foldLeft(Set[(Int, Int)]()) { (set, idx) =>
      if (set.size > 6) { set }
      else {
        set + (n -> rand.nextInt(nodes))
      }
    }
  }.toSeq

  val MaxWeight = 2
  val weightedEdges = (0 to nodes).flatMap { n =>
    // try to get at least 10 edges for each node
    (0 to ((nodes / 5) max (10))).foldLeft(Set[(Int, Int, Double)]()) { (set, idx) =>
      if (set.size > 10) { set }
      else {
        set + ((n, rand.nextInt(nodes), rand.nextDouble * MaxWeight))
      }
    }
  }.toSeq

  def cosineOf(es: Seq[(Int, Int)]): Map[(Int, Int), Double] = {
    // Get followers of each node:
    val matrix: Map[Int, Map[Int, Double]] =
      es.groupBy { _._2 }.mapValues { seq => seq.map { case (from, to) => (from, 1.0) }.toMap }
    for (
      (k1, v1) <- matrix if (k1 % 2 == 0);
      (k2, v2) <- matrix if (k2 % 2 == 1)
    ) yield ((k1, k2) -> (dot(v1, v2) / scala.math.sqrt(dot(v1, v1) * dot(v2, v2))))
  }

  def weightedCosineOf(es: Seq[(Int, Int, Double)]): Map[(Int, Int), Double] = {
    // Get followers of each node:
    val matrix: Map[Int, Map[Int, Double]] =
      es.groupBy { _._2 }.mapValues { seq => seq.map { case (from, to, weight) => (from, weight) }.toMap }
    for (
      (k1, v1) <- matrix if (k1 % 2 == 0);
      (k2, v2) <- matrix if (k2 % 2 == 1)
    ) yield ((k1, k2) -> (dot(v1, v2) / scala.math.sqrt(dot(v1, v1) * dot(v2, v2))))
  }

  "A TypedCosineJob" should {
    import Dsl._
    "compute cosine similarity" in {
      JobTest(new TypedCosineSimJob(_))
        .source(TypedTsv[(Int, Int)]("ingraph"), edges)
        .sink[(Int, Int, Double)](TypedTsv[(Int, Int, Double)]("out")) { ob =>
          val result = ob.map { case (n1, n2, d) => ((n1 -> n2) -> d) }.toMap
          val error = Group.minus(result, cosineOf(edges))
          dot(error, error) should be < 0.001
        }
        .run
        .finish()
    }
    "compute dimsum cosine similarity" in {
      JobTest(new TypedDimsumCosineSimJob(_))
        .source(TypedTsv[(Int, Int, Double)]("ingraph"), weightedEdges)
        .sink[(Int, Int, Double)](TypedTsv[(Int, Int, Double)]("out")) { ob =>
          val result = ob.map { case (n1, n2, d) => ((n1 -> n2) -> d) }.toMap
          val error = Group.minus(result, weightedCosineOf(weightedEdges))
          dot(error, error) should be < (0.01 * error.size)
        }
        .run
        .finish()
    }
  }
}
