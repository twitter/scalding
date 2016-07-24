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
package com.twitter.scalding.examples

import scala.collection._

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.Dsl._

import WeightedPageRankFromMatrixSpec._

class WeightedPageRankFromMatrixSpec extends WordSpec with Matchers {

  "Weighted PageRank from Matrix job" should {

    // 0.0 0.0 0.0 0.0 1.0
    // 0.5 0.0 0.0 0.0 0.0
    // 0.5 0.0 0.0 0.0 0.0
    // 0.0 1.0 0.5 0.0 0.0
    // 0.0 0.0 0.5 1.0 0.0
    val edges = List(
      (0, 4, 1.0),
      (1, 0, 0.5),
      (2, 0, 0.5),
      (3, 1, 1.0),
      (3, 2, 0.5),
      (4, 2, 0.5),
      (4, 3, 1.0))

    val d = 0.4d // damping factor
    val n = 5 // number of nodes
    val onesVector = filledColumnVector(1d, n)
    val iterationZeroVector = filledColumnVector(1d / n, n)

    val expectedSolution = Array(0.28, 0.173333, 0.173333, 0.173333, 0.2)

    JobTest(new WeightedPageRankFromMatrix(_))
      .arg("d", d.toString)
      .arg("n", n.toString)
      .arg("convergenceThreshold", "0.0001")
      .arg("maxIterations", "1")
      .arg("currentIteration", "0")
      .arg("rootDir", "root")
      .source(TypedTsv[(Int, Int, Double)]("root/edges"), edges)
      .source(TypedTsv[(Int, Double)]("root/onesVector"), onesVector)
      .source(TypedTsv[(Int, Double)]("root/iterations/0"), iterationZeroVector)
      .sink[(Int, Int, Double)](Tsv("root/constants/M_hat")) { outputBuffer =>
        outputBuffer should have size 7
        val outputMap = toSparseMap(outputBuffer)
        outputMap((0 -> 1)) shouldBe 0.4
        outputMap((0 -> 2)) shouldBe 0.4
        outputMap((1 -> 3)) shouldBe 0.26666 +- 0.00001
        outputMap((2 -> 3)) shouldBe 0.13333 +- 0.00001
        outputMap((2 -> 4)) shouldBe 0.13333 +- 0.00001
        outputMap((3 -> 4)) shouldBe 0.26666 +- 0.00001
        outputMap((4 -> 0)) shouldBe 0.4
      }
      .sink[(Int, Double)](Tsv("root/constants/priorVector")) { outputBuffer =>
        outputBuffer should have size 5
        val expectedValue = ((1 - d) / 2) * d
        assertVectorsEqual(
          new Array[Double](5).map { v => expectedValue },
          outputBuffer.map(_._2).toArray)
      }
      .sink[(Int, Double)](Tsv("root/iterations/1")) { outputBuffer =>
        outputBuffer should have size 5
        assertVectorsEqual(
          expectedSolution,
          outputBuffer.map(_._2).toArray,
          0.00001)
      }
      .typedSink(TypedTsv[Double]("root/diff")) { outputBuffer =>
        outputBuffer should have size 1

        val expectedDiff =
          expectedSolution.zip(iterationZeroVector.map(_._2)).
            map { case (a, b) => math.abs(a - b) }.
            sum
        outputBuffer.head shouldBe expectedDiff +- 0.00001
      }
      .run
      .finish()
  }

  private def assertVectorsEqual(expected: Array[Double], actual: Array[Double], variance: Double): Unit = {
    actual.zipWithIndex.foreach {
      case (value, i) =>
        value shouldBe (expected(i)) +- variance
    }
  }

  private def assertVectorsEqual(expected: Array[Double], actual: Array[Double]): Unit = {
    actual.zipWithIndex.foreach {
      case (value, i) =>
        value shouldBe (expected(i))
    }
  }
}

object WeightedPageRankFromMatrixSpec {

  def toSparseMap[Row, Col, V](iterable: Iterable[(Row, Col, V)]): Map[(Row, Col), V] =
    iterable.map { entry => ((entry._1, entry._2), entry._3) }.toMap

  def filledColumnVector(value: Double, size: Int): List[(Int, Double)] = {
    val vector = mutable.ListBuffer[(Int, Double)]()
    (0 until size).foreach { row =>
      vector += new Tuple2(row, value)
    }

    vector.toList
  }
}

/**
 * Octave/Matlab implementations to provide the expected ranks. This comes from
 * the Wikipedia page on PageRank:
 * http://en.wikipedia.org/wiki/PageRank#Computation
 *
 * function [v] = iterate(A, sv, d)
 *
 * N = size(A, 2)
 * M = (spdiags(1 ./ sum(A, 2), 0, N, N) * A)';
 * v = (d * M * sv) + (((1 - d) / N) .* ones(N, 1));
 *
 * endfunction
 *
 * iterate([0 0 0 0 1; 0.5 0 0 0 0; 0.5 0 0 0 0; 0 1 0.5 0 0; 0 0 0.5 1 0], [0.2; 0.2; 0.2; 0.2; 0.2], 0.4)
 *
 * % Parameter M adjacency matrix where M_i,j represents the link from 'j' to 'i', such that for all 'j' sum(i, M_i,j) = 1
 * % Parameter d damping factor
 * % Parameter v_quadratic_error quadratic error for v
 * % Return v, a vector of ranks such that v_i is the i-th rank from [0, 1]
 *
 * function [v] = rank(M, d, v_quadratic_error)
 *
 * N = size(M, 2); % N is equal to half the size of M
 * v = rand(N, 1);
 * v = v ./ norm(v, 2);
 * last_v = ones(N, 1) * inf;
 * M_hat = (d .* M) + (((1 - d) / N) .* ones(N, N));
 *
 * while(norm(v - last_v, 2) > v_quadratic_error)
 * last_v = v;
 * v = M_hat * v;
 * v = v ./ norm(v, 2);
 * end
 *
 * endfunction
 *
 * M = [0 0 0 0 1 ; 0.5 0 0 0 0 ; 0.5 0 0 0 0 ; 0 1 0.5 0 0 ; 0 0 0.5 1 0];
 * rank(M, 0.4, 0.001)
 *
 */
