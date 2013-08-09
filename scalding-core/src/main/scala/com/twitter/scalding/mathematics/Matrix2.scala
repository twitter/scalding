/*
Copyright 2013 Tomas Tauber

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

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.TDsl._
import com.twitter.scalding._
import com.twitter.algebird.{ Monoid, Ring, Group }
import scala.collection.mutable.HashMap

/**
 * *************
 * * WARNING: This is a new experimental API. Some features are missing
 * * and expect it to break. Use the old Matrix API if you do not feel adventurous.
 * **************
 */
sealed trait Matrix2[R, C, V] {
  implicit def rowOrd: Ordering[R]
  implicit def colOrd: Ordering[C]
  val sizeHint: SizeHint = NoClue
  def +(that: Matrix2[R, C, V])(implicit mon: Monoid[V]): Matrix2[R, C, V] = Sum(this, that, mon)
  // TODO: optimize difference
  def -(that: Matrix2[R, C, V])(implicit g: Group[V]): Matrix2[R, C, V] = Sum(this, that.negate, g)
  def negate(implicit g: Group[V]): Matrix2[R, C, V] = MatrixLiteral(toTypedPipe.map(x => (x._1, x._2, g.negate(x._3))), sizeHint)
  // TODO: Hadamard product
  def #*#(that: Matrix2[R, C, V]): Matrix2[R, C, V] = sys.error("todo")
  // Matrix product
  def *[C2](that: Matrix2[C, C2, V])(implicit ring: Ring[V]): Matrix2[R, C2, V] = Product(this, that, false, ring)
  def toTypedPipe: TypedPipe[(R, C, V)]
  def transpose: Matrix2[C, R, V]
  def optimizedSelf: Matrix2[R, C, V] = Matrix2.optimize(this.asInstanceOf[Matrix2[Any, Any, V]])._2.asInstanceOf[Matrix2[R, C, V]]
  // TODO: complete the rest of the API to match the old Matrix API (many methods are effectively on the TypedPipe)
}

case class Product[R, C, C2, V](left: Matrix2[R, C, V], right: Matrix2[C, C2, V], optimal: Boolean = false, ring: Ring[V]) extends Matrix2[R, C2, V] {
  def toTypedPipe: TypedPipe[(R, C2, V)] = {
    if (optimal) {
      val ord: Ordering[C] = left.colOrd
      val ord2: Ordering[(R, C2)] = Ordering.Tuple2(rowOrd, colOrd)
      // TODO: pick the best joining algorithm based the sizeHint
      val one = left.toTypedPipe.groupBy(x => x._2)(ord)
      val two = right.toTypedPipe.groupBy(x => x._1)(ord)

      one.join(two).mapValues { case (l, r) => (l._1, r._2, ring.times(l._3, r._3)) }.values.
        groupBy(w => (w._1, w._2))(ord2).mapValues { _._3 }
        .sum(ring)
        .filter { kv => ring.isNonZero(kv._2) }
        .map { case ((r, c), v) => (r, c, v) }

    } else {
      optimizedSelf.toTypedPipe
    }
  }

  override val sizeHint = left.sizeHint * right.sizeHint

  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C2] = right.colOrd
  override lazy val transpose: Product[C2, C, R, V] = Product(right.transpose, left.transpose, false, ring)
}

case class Sum[R, C, V](left: Matrix2[R, C, V], right: Matrix2[R, C, V], mon: Monoid[V]) extends Matrix2[R, C, V] {
  def toTypedPipe: TypedPipe[(R, C, V)] = {
    if (left.equals(right)) {
      left.optimizedSelf.toTypedPipe.map(v => (v._1, v._2, mon.plus(v._3, v._3)))
    } else {
      val ord: Ordering[(R, C)] = Ordering.Tuple2(left.rowOrd, left.colOrd)
      // TODO: if left or right are Sums, Sum of all of them can be done in one groupBy -> flatten the tree of Sums into a List[Sum]
      (left.optimizedSelf.toTypedPipe ++ right.optimizedSelf.toTypedPipe)
        .groupBy(x => (x._1, x._2))(ord).mapValues { _._3 }
        .sum(mon)
        .filter { kv => mon.isNonZero(kv._2) }
        .map { case ((r, c), v) => (r, c, v) }
    }
  }

  override val sizeHint = left.sizeHint + right.sizeHint

  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C] = left.colOrd
  override lazy val transpose: Sum[C, R, V] = Sum(left.transpose, right.transpose, mon)
}

case class MatrixLiteral[R, C, V](override val toTypedPipe: TypedPipe[(R, C, V)], override val sizeHint: SizeHint)(implicit override val rowOrd: Ordering[R], override val colOrd: Ordering[C]) extends Matrix2[R, C, V] {
  override lazy val transpose: MatrixLiteral[C, R, V] = MatrixLiteral(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)(colOrd, rowOrd)
}

object Matrix2 {

  /**
   * The original prototype that employs the standard O(n^3) dynamic programming
   * procedure to optimize a matrix chain factorization
   */
  def optimizeProductChain[V](p: IndexedSeq[Matrix2[Any, Any, V]], ring: Option[Ring[V]]): (Long, Matrix2[Any, Any, V]) = {

    val subchainCosts = HashMap.empty[(Int, Int), Long]

    val splitMarkers = HashMap.empty[(Int, Int), Int]

    def computeCosts(p: IndexedSeq[Matrix2[Any, Any, V]], i: Int, j: Int): Long = {
      if (subchainCosts.contains((i, j))) subchainCosts((i, j))
      if (i == j) subchainCosts.put((i, j), 0)
      else {
        subchainCosts.put((i, j), Long.MaxValue)
        for (k <- i to (j - 1)) {
          val cost = computeCosts(p, i, k) + computeCosts(p, k + 1, j) +
            (p(i).sizeHint * (p(k).sizeHint * p(j).sizeHint)).total.getOrElse(0L)
          if (cost < subchainCosts((i, j))) {
            subchainCosts.put((i, j), cost)
            splitMarkers.put((i, j), k)
          }
        }
      }

      subchainCosts((i, j))
    }

    def generatePlan(i: Int, j: Int): Matrix2[Any, Any, V] = {
      if (i == j) p(i)
      else {
        val k = splitMarkers((i, j))
        val left = generatePlan(i, k)
        val right = generatePlan(k + 1, j)
        Product(left, right, true, ring.get)
      }

    }

    val best = computeCosts(p, 0, p.length - 1)

    (best, generatePlan(0, p.length - 1))
  }

  /**
   * This function walks the input tree, finds basic blocks to optimize,
   * i.e. matrix product chains that are not interrupted by summations.
   * One example:
   * A*B*C*(D+E)*(F*G) => "basic blocks" are ABC, D, E, and FG
   *
   * + it now does "global" optimization - i.e. over optimize over basic blocks.
   * In the above example, we'd treat (D+E) as a temporary matrix T and optimize the whole chain ABCTFG
   *
   * Not sure if making use of distributivity to generate more variants would be good.
   * In the above example, we could also generate ABCDFG + ABCEFG and have basic blocks: ABCDFG, and ABCEFG.
   * But this would be almost twice as much work with the current cost estimation.
   */
  def optimize[V](mf: Matrix2[Any, Any, V]): (Long, Matrix2[Any, Any, V]) = {

    /**
     * Recursive function - returns a flatten product chain and optimizes product chains under sums
     */
    def optimizeBasicBlocks(mf: Matrix2[Any, Any, V]): (List[Matrix2[Any, Any, V]], Long, Option[Ring[V]]) = {
      mf match {
        // basic block of one matrix
        case element: MatrixLiteral[Any, Any, V] => (List(element), 0, None)
        // two potential basic blocks connected by a sum
        case Sum(left, right, mon) => {
          val (lastLChain, lastCost1, ringL) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, ringR) = optimizeBasicBlocks(right)
          val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq, ringL)
          val (cost2, newRight) = optimizeProductChain(lastRChain.toIndexedSeq, ringR)
          (List(Sum(newLeft, newRight, mon)), lastCost1 + lastCost2 + cost1 + cost2, ringL.orElse(ringR))
        }
        // chain (...something...)*(...something...)
        case Product(left, right, _, ring) => {
          val (lastLChain, lastCost1, ringL) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, ringR) = optimizeBasicBlocks(right)
          (lastLChain ++ lastRChain, lastCost1 + lastCost2, Some(ring))
        }
      }
    }
    val (lastChain, lastCost, ring) = optimizeBasicBlocks(mf)
    val (potentialCost, finalResult) = optimizeProductChain(lastChain.toIndexedSeq, ring)
    (lastCost + potentialCost, finalResult)
  }

}
