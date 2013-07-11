package com.twitter.scalding.mathematics

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.TDsl._
import com.twitter.scalding._
import com.twitter.algebird.{ Monoid, Ring }
import scala.collection.mutable.HashMap

object Matrix2 {
  sealed abstract class Matrix2[RC,V](val sizeHint: SizeHint = NoClue) {
    implicit val ring: Ring[V]
    implicit val ord1: Ordering[RC]
    implicit val ord2: Ordering[(RC,RC)]
    def +(that: Matrix2[RC,V]): Matrix2[RC,V] = Sum(this, that)(ring, ord1, ord2)
    def *(that: Matrix2[RC,V]): Matrix2[RC,V] = Product(this, that, false)(ring, ord1, ord2)
    val tpipe: TypedPipe[(RC, RC, V)]
    def transpose: Matrix2[RC,V] = Literal(tpipe.map(x => (x._2, x._1, x._3)), sizeHint)(ring,ord1,ord2)
    lazy val optimizedSelf = optimize(this)._2
  }

  case class Product[RC,V](left: Matrix2[RC,V], right: Matrix2[RC,V], optimal: Boolean = false)(override val ring: Ring[V], override val ord1: Ordering[RC], override val ord2: Ordering[(RC,RC)]) extends Matrix2[RC,V] {
    def toPipe(): TypedPipe[(RC, RC, V)] = {
      if (optimal) {
        // TODO: pick the best joining algorithm based the sizeHint
        val one = left.tpipe.groupBy(x => x._2)(ord1)
        val two = right.tpipe.groupBy(x => x._1)(ord1)

        one.join(two).mapValues { case (l, r) => (l._1, r._2, ring.times(l._3, r._3)) }.values.
          groupBy(w => (w._1, w._2))(ord2).mapValues { _._3 }
          .sum(ring)
          .filter { kv => ring.isNonZero(kv._2) }
          .map { case ((r, c), v) => (r, c, v) }

      } else {
        optimizedSelf.tpipe
      }
    }

    override lazy val tpipe = toPipe()
    override val sizeHint = left.sizeHint * right.sizeHint
  }

  case class Sum[RC, V](left: Matrix2[RC, V], right: Matrix2[RC, V])(override val ring: Ring[V], override val ord1: Ordering[RC], override val ord2: Ordering[(RC,RC)]) extends Matrix2[RC, V] {
    def toPipe(): TypedPipe[(RC, RC, V)] = {
      if (left.equals(right)) {
        left.optimizedSelf.tpipe.map(v => (v._1, v._2, ring.plus(v._3, v._3)))
      } else {
        (left.optimizedSelf.tpipe ++ right.optimizedSelf.tpipe).groupBy(x => (x._1, x._2))(ord2).mapValues { _._3 }
          .sum(ring)
          .filter { kv => ring.isNonZero(kv._2) }
          .map { case ((r, c), v) => (r, c, v) }
      }
    }

    override lazy val tpipe = toPipe()
    override val sizeHint = left.sizeHint + right.sizeHint
  }

  case class Literal[RC,V](override val tpipe: TypedPipe[(RC, RC, V)], override val sizeHint: SizeHint)(override val ring: Ring[V], override val ord1: Ordering[RC], override val ord2: Ordering[(RC,RC)]) extends Matrix2[RC, V]
  
  /**
   * The original prototype that employs the standard O(n^3) dynamic programming
   * procedure to optimize a matrix chain factorization
   */
  def optimizeProductChain[RC,V](p: IndexedSeq[Literal[RC,V]])(implicit ring: Ring[V], ord1: Ordering[RC], ord2: Ordering[(RC,RC)]): (Long, Matrix2[RC,V]) = {

    val subchainCosts = HashMap.empty[(Int, Int), Long]

    val splitMarkers = HashMap.empty[(Int, Int), Int]

    def computeCosts(p: IndexedSeq[Literal[RC,V]], i: Int, j: Int): Long = {
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

    def generatePlan(i: Int, j: Int): Matrix2[RC,V] = {
      if (i == j) p(i)
      else {
        val k = splitMarkers((i, j))
        val left = generatePlan(i, k)
        val right = generatePlan(k + 1, j)
        val result = Product(left, right, true)(ring, ord1, ord2)
        result
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
   * TODO: "global" optimization - i.e. over optimize over basic blocks. In the above example, we'd treat (D+E) as a temporary matrix T and optimize the whole chain ABCTFG
   * TODO: make use of distributivity to generate more variants. In the above example, we could also generate ABCDFG + ABCEFG and have basic blocks: ABCDFG, and ABCEFG
   */
  def optimize[RC,V](mf: Matrix2[RC,V])(implicit ring: Ring[V], ord1: Ordering[RC], ord2: Ordering[(RC,RC)]): (Long, Matrix2[RC,V]) = {

    /**
     * Helper function that either returns an optimized product chain
     * or the last visited place in the tree
     */
    def chainOrLast(chain: List[Literal[RC,V]], last: Option[(Long, Matrix2[RC,V])]): (Long, Matrix2[RC,V]) = {
      if (chain.isEmpty) last.get
      else optimizeProductChain(chain.toIndexedSeq)
    }

    /**
     * Recursive function - returns a flatten product chain so far and the rest of the connected tree
     */
    def optimizeBasicBlocks(mf: Matrix2[RC,V]): (List[Literal[RC,V]], Option[(Long, Matrix2[RC,V])]) = {
      mf match {
        // basic block of one matrix
        case element: Literal[RC,V] => (List(element), None)
        // two potential basic blocks connected by a sum
        case Sum(left, right) => {
          val (lastLChain, leftTemp) = optimizeBasicBlocks(left)
          val (lastRChain, rightTemp) = optimizeBasicBlocks(right)
          val (cost1, newLeft) = chainOrLast(lastLChain, leftTemp)
          val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
          (Nil, Some(cost1 + cost2, Sum(newLeft, newRight)(ring, ord1, ord2)))
        }
        // basic block A*B
        case Product(leftp: Literal[RC,V], rightp: Literal[RC,V], _) => {
          (List(leftp, rightp), None)
        }
        // potential chain (...something...)*right or just two basic blocks connected by a product
        case Product(left: Product[RC,V], right: Literal[RC,V], _) => {
          val (lastLChain, leftTemp) = optimizeBasicBlocks(left)
          if (lastLChain.isEmpty) {
            val (cost, newLeft) = leftTemp.get
            val interProduct = Product(newLeft, right, true)(ring, ord1, ord2)
            (Nil, Some(cost, interProduct))
          } else {
            (lastLChain ++ List(right), leftTemp)
          }
        }
        // potential chain left*(...something...) or just two basic blocks connected by a product
        case Product(left: Literal[RC,V], right: Product[RC,V], _) => {
          val (lastRChain, rightTemp) = optimizeBasicBlocks(right)
          if (lastRChain.isEmpty) {
            val (cost, newRight) = rightTemp.get
            val interProduct = Product(left, newRight, true)(ring, ord1, ord2)
            (Nil, Some(cost, interProduct))
          } else {
            (left :: lastRChain, rightTemp)
          }
        }
        // potential chain (...something...)*(...something...) or just two basic blocks connected by a product
        case Product(left, right, _) => {
          val (lastLChain, leftTemp) = optimizeBasicBlocks(left)
          val (lastRChain, rightTemp) = optimizeBasicBlocks(right)
          if (lastLChain.isEmpty) {
            val (cost1, newLeft) = leftTemp.get
            val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
            (Nil, Some(cost1 + cost2, Product(newLeft, newRight, true)(ring, ord1, ord2)))
          } else {
            if (lastRChain.isEmpty) {
              val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq)
              val (cost2, newRight) = rightTemp.get
              (Nil, Some(cost1 + cost2, Product(newLeft, newRight, true)(ring, ord1, ord2)))
            } else {
              (lastLChain ++ lastRChain, None)
            }
          }
        }
      }
    }
    val (lastChain, form) = optimizeBasicBlocks(mf)

    chainOrLast(lastChain, form)
  }

}
