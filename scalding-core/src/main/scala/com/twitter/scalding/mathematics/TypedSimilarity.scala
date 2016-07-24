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

import com.twitter.scalding.typed.{ Grouped, TypedPipe, WithReducers }

import java.io.Serializable

/**
 * Implementation of DISCO and DIMSUM approximation similarity algorithm
 * @author Oscar Boykin
 * @author Kevin Lin
 */

/**
 * Represents an Edge in a graph with some edge data
 */
case class Edge[+N, +E](from: N, to: N, data: E) {
  def mapData[F](fn: (E => F)): Edge[N, F] = Edge(from, to, fn(data))
  def reverse: Edge[N, E] = Edge(to, from, data)
}

abstract sealed trait Degree { val degree: Int }
case class InDegree(override val degree: Int) extends Degree
case class OutDegree(override val degree: Int) extends Degree
case class Weight(weight: Double)
case class L2Norm(norm: Double)

object GraphOperations extends Serializable {
  /**
   * For each N, aggregate all the edges, and attach Edge state
   */
  def joinAggregate[N, E, T](grouped: Grouped[N, Edge[N, E]])(agfn: Iterable[Edge[N, E]] => T): TypedPipe[Edge[N, (E, T)]] =
    grouped.cogroup(grouped) {
      (to: N, left: Iterator[Edge[N, E]], right: Iterable[Edge[N, E]]) =>
        val newState = agfn(right)
        left.map { _.mapData { e: E => (e, newState) } }
    }
      .values

  // Returns all Vertices with non-zero in-degree
  def withInDegree[N, E](g: TypedPipe[Edge[N, E]])(implicit ord: Ordering[N]): TypedPipe[Edge[N, (E, InDegree)]] = joinAggregate(g.groupBy { _.to }) { it =>
    InDegree(it.size)
  }

  // Returns all Vertices with non-zero out-degree
  def withOutDegree[N, E](g: TypedPipe[Edge[N, E]])(implicit ord: Ordering[N]): TypedPipe[Edge[N, (E, OutDegree)]] = joinAggregate(g.groupBy { _.from }) { it =>
    OutDegree(it.size)
  }

  // Returns all Vertices with weights and non-zero norms
  def withInNorm[N, E](g: TypedPipe[Edge[N, Weight]])(implicit ord: Ordering[N]): TypedPipe[Edge[N, (Weight, L2Norm)]] = joinAggregate(g.groupBy { _.to }) { it =>
    val norm = scala.math.sqrt(
      it.iterator.map { a =>
        val x = a.data.weight
        x * x
      }.sum)

    L2Norm(norm)
  }
}

case class SetSimilarity(intersection: Int, sizeLeft: Int, sizeRight: Int) {
  lazy val cosine: Option[Double] =
    if (intersection == 0)
      Some(0.0)
    else {
      val denom = scala.math.sqrt(sizeLeft.toDouble * sizeRight.toDouble)
      if (denom == 0.0) {
        None
      } else {
        Some(intersection.toDouble / denom)
      }
    }
}

trait TypedSimilarity[N, E, S] extends Serializable {
  def nodeOrdering: Ordering[N]
  /**
   * Given a TypedPipe of edges, and a predicate for a smaller group (smallpred) of nodes
   * and a bigger group (bigpred), compute the similarity between each item in the two sets
   * The Edge.from nodes in the result will all satisfy smallpred, and the Edge.to will
   * all satisfy bigpred. It is more efficient if you keep the smallpred set smaller.
   */
  def apply(g: TypedPipe[Edge[N, E]],
    smallpred: N => Boolean,
    bigpred: N => Boolean): TypedPipe[Edge[N, S]]
  // Do similarity on all the nodes
  def apply(g: TypedPipe[Edge[N, E]]): TypedPipe[Edge[N, S]] = {
    val always = { n: N => true }
    apply(g, always, always)
  }
}

object TypedSimilarity extends Serializable {
  private def maybeWithReducers[T <: WithReducers[T]](withReds: T, reds: Option[Int]) =
    reds match {
      case Some(i) => withReds.withReducers(i)
      case None => withReds
    }

  // key: document,
  // value: (word, documentsWithWord)
  // return: Edge of similarity between words measured by documents
  def exactSetSimilarity[N: Ordering](g: Grouped[N, (N, Int)],
    smallpred: N => Boolean, bigpred: N => Boolean): TypedPipe[Edge[N, SetSimilarity]] =
    /* E_{ij} = 1 if document -> word exists
     * (E^T E)_ij = # of shared documents of i,j
     * = \sum_k E_ki E_kj
     */
    // First compute (i,j) => E_{ki} E_{kj}
    maybeWithReducers(g.join(g)
      .values
      .flatMap {
        case ((node1, deg1), (node2, deg2)) =>
          if (smallpred(node1) && bigpred(node2)) Some(((node1, node2), (1, deg1, deg2))) else None
      }
      .group, g.reducers)
      // Use reduceLeft to push to reducers, no benefit in mapside here
      .reduceLeft { (left, right) =>
        // The degrees we always take the left:
        val (leftCnt, deg1, deg2) = left
        (leftCnt + right._1, deg1, deg2)
      }
      .map {
        case ((node1, node2), (cnt, deg1, deg2)) =>
          Edge(node1, node2, SetSimilarity(cnt, deg1, deg2))
      }

  /*
   * key: document,
   * value: (word, documentsWithWord)
   * return: Edge of similarity between words measured by documents
   * See: http://arxiv.org/pdf/1206.2082v2.pdf
   */
  def discoCosineSimilarity[N: Ordering](smallG: Grouped[N, (N, Int)],
    bigG: Grouped[N, (N, Int)], oversample: Double): TypedPipe[Edge[N, Double]] = {
    // 1) make rnd lazy due to serialization,
    // 2) fix seed so that map-reduce speculative execution does not give inconsistent results.
    lazy val rnd = new scala.util.Random(1024)
    maybeWithReducers(smallG.cogroup(bigG) { (n: N, leftit: Iterator[(N, Int)], rightit: Iterable[(N, Int)]) =>
      // Use a co-group to ensure this happens in the reducer:
      leftit.flatMap {
        case (node1, deg1) =>
          rightit.iterator.flatMap {
            case (node2, deg2) =>
              val weight = 1.0 / scala.math.sqrt(deg1.toDouble * deg2.toDouble)
              val prob = oversample * weight
              if (prob >= 1.0) {
                // Small degree case, just output all of them:
                Iterator(((node1, node2), weight))
              } else if (rnd.nextDouble < prob) {
                // Sample
                Iterator(((node1, node2), 1.0 / oversample))
              } else
                Iterator.empty
          }
      }
    }
      .values
      .group, smallG.reducers)
      .forceToReducers
      .sum
      .map { case ((node1, node2), sim) => Edge(node1, node2, sim) }
  }

  /*
   * key: document,
   * value: (word, word weight in the document, norm of the word)
   * return: Edge of similarity between words measured by documents
   * See: http://stanford.edu/~rezab/papers/dimsum.pdf
   */
  def dimsumCosineSimilarity[N: Ordering](smallG: Grouped[N, (N, Double, Double)],
    bigG: Grouped[N, (N, Double, Double)], oversample: Double): TypedPipe[Edge[N, Double]] = {
    lazy val rnd = new scala.util.Random(1024)
    maybeWithReducers(smallG.cogroup(bigG) { (n: N, leftit: Iterator[(N, Double, Double)], rightit: Iterable[(N, Double, Double)]) =>
      // Use a co-group to ensure this happens in the reducer:
      leftit.flatMap {
        case (node1, weight1, norm1) =>
          rightit.iterator.flatMap {
            case (node2, weight2, norm2) =>
              val weight = 1.0 / (norm1 * norm2)
              val prob = oversample * weight
              if (prob >= 1.0) {
                // Small degree case, just output all of them:
                Iterator(((node1, node2), weight * weight1 * weight2))
              } else if (rnd.nextDouble < prob) {
                // Sample
                Iterator(((node1, node2), 1.0 / oversample * weight1 * weight2))
              } else
                Iterator.empty
          }
      }
    }
      .values
      .group, smallG.reducers)
      .forceToReducers
      .sum
      .map { case ((node1, node2), sim) => Edge(node1, node2, sim) }
  }
}

/**
 * This algothm is just matrix multiplication done by hand to make it
 * clearer when we do the sampling implementation
 */
class ExactInCosine[N](reducers: Int = -1)(implicit override val nodeOrdering: Ordering[N]) extends TypedSimilarity[N, InDegree, Double] {

  def apply(graph: TypedPipe[Edge[N, InDegree]],
    smallpred: N => Boolean, bigpred: N => Boolean): TypedPipe[Edge[N, Double]] = {
    val groupedOnSrc = graph
      .filter { e => smallpred(e.to) || bigpred(e.to) }
      .map { e => (e.from, (e.to, e.data.degree)) }
      .group
      .withReducers(reducers)
    TypedSimilarity.exactSetSimilarity(groupedOnSrc, smallpred, bigpred)
      .flatMap { e => e.data.cosine.map { c => e.mapData { s => c } } }
  }
}

/**
 * Params:
 * minCos: the minimum cosine similarity you care about accuracy for
 * delta: the error on the approximated cosine (e.g. 0.05 = 5%)
 * boundedProb: the probability we have larger than delta error
 * see: http://arxiv.org/pdf/1206.2082v2.pdf for more details
 */
class DiscoInCosine[N](minCos: Double, delta: Double, boundedProb: Double, reducers: Int = -1)(implicit override val nodeOrdering: Ordering[N]) extends TypedSimilarity[N, InDegree, Double] {

  // The probability of being more than delta error is approx:
  // boundedProb ~ exp(-p delta^2 / 2)
  private val oversample = (-2.0 * scala.math.log(boundedProb) / (delta * delta)) / minCos

  def apply(graph: TypedPipe[Edge[N, InDegree]],
    smallpred: N => Boolean, bigpred: N => Boolean): TypedPipe[Edge[N, Double]] = {
    val bigGroupedOnSrc = graph
      .filter { e => bigpred(e.to) }
      .map { e => (e.from, (e.to, e.data.degree)) }
      .group
      .withReducers(reducers)
    val smallGroupedOnSrc = graph
      .filter { e => smallpred(e.to) }
      .map { e => (e.from, (e.to, e.data.degree)) }
      .group
      .withReducers(reducers)

    TypedSimilarity.discoCosineSimilarity(smallGroupedOnSrc, bigGroupedOnSrc, oversample)
  }

}

class DimsumInCosine[N](minCos: Double, delta: Double, boundedProb: Double, reducers: Int = -1)(implicit override val nodeOrdering: Ordering[N]) extends TypedSimilarity[N, (Weight, L2Norm), Double] {

  // The probability of being more than delta error is approx:
  // boundedProb ~ exp(-p delta^2 / 2)
  private val oversample = (-2.0 * scala.math.log(boundedProb) / (delta * delta)) / minCos

  def apply(graph: TypedPipe[Edge[N, (Weight, L2Norm)]],
    smallpred: N => Boolean, bigpred: N => Boolean): TypedPipe[Edge[N, Double]] = {
    val bigGroupedOnSrc = graph
      .filter { e => bigpred(e.to) }
      .map { e => (e.from, (e.to, e.data._1.weight, e.data._2.norm)) }
      .group
      .withReducers(reducers)
    val smallGroupedOnSrc = graph
      .filter { e => smallpred(e.to) }
      .map { e => (e.from, (e.to, e.data._1.weight, e.data._2.norm)) }
      .group
      .withReducers(reducers)

    TypedSimilarity.dimsumCosineSimilarity(smallGroupedOnSrc, bigGroupedOnSrc, oversample)
  }
}
