package com.twitter.scalding.mathematics

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.TDsl._
import com.twitter.scalding._
import com.twitter.algebird.{ Monoid, Ring }
import scala.collection.mutable.HashMap

  sealed trait Matrix2[R,C,V] {
	implicit def rowOrd: Ordering[R]
	implicit def colOrd: Ordering[C]   
    val sizeHint: SizeHint = NoClue
    def +(that: Matrix2[R,C,V])(implicit ring: Ring[V]): Matrix2[R,C,V] = Sum(this, that, ring)
    def *[C2](that: Matrix2[C,C2,V])(implicit ring: Ring[V]): Matrix2[R,C2,V] = Product(this, that, false, ring)
    def toTypedPipe: TypedPipe[(R, C, V)]
    def transpose: Matrix2[C,R,V] = Literal(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)
    def optimizedSelf()(implicit ring: Ring[V]) = Matrix2.optimize(this.asInstanceOf[Matrix2[Any,Any,V]])(ring)._2
  }

  case class Product[R, C, C2, V](left: Matrix2[R,C,V], right: Matrix2[C,C2,V], optimal: Boolean = false, ring: Ring[V]) extends Matrix2[R,C2,V] {
    def toTypedPipe: TypedPipe[(R, C2, V)] = {
      if (optimal) {
        val ord: Ordering[C] = left.colOrd
        val ord2: Ordering[(R,C2)] = Ordering.Tuple2(rowOrd, colOrd)
        // TODO: pick the best joining algorithm based the sizeHint
        val one = left.toTypedPipe.groupBy(x => x._2)(ord)
        val two = right.toTypedPipe.groupBy(x => x._1)(ord)

        one.join(two).mapValues { case (l, r) => (l._1, r._2, ring.times(l._3, r._3)) }.values.
          groupBy(w => (w._1, w._2))(ord2).mapValues { _._3 }
          .sum(ring)
          .filter { kv => ring.isNonZero(kv._2) }
          .map { case ((r, c), v) => (r, c, v) }

      } else {
        optimizedSelf()(ring).asInstanceOf[Matrix2[R,C2,V]].toTypedPipe
      }
    }

    override val sizeHint = left.sizeHint * right.sizeHint
    
    implicit override val rowOrd: Ordering[R] = left.rowOrd
    implicit override val colOrd: Ordering[C2] = right.colOrd    
  }

  case class Sum[R, C, V](left: Matrix2[R, C, V], right: Matrix2[R, C, V], ring: Ring[V]) extends Matrix2[R, C, V] {
    def toTypedPipe: TypedPipe[(R, C, V)] = {
      if (left.equals(right)) {
        left.optimizedSelf()(ring).asInstanceOf[Matrix2[R,C,V]].toTypedPipe.map(v => (v._1, v._2, ring.plus(v._3, v._3)))
      } else {
        val ord: Ordering[(R,C)] = Ordering.Tuple2(left.rowOrd, left.colOrd)
        (left.optimizedSelf()(ring).asInstanceOf[Matrix2[R,C,V]].toTypedPipe ++ right.optimizedSelf()(ring).asInstanceOf[Matrix2[R,C,V]].toTypedPipe)
          .groupBy(x => (x._1, x._2))(ord).mapValues { _._3 }
          .sum(ring)
          .filter { kv => ring.isNonZero(kv._2) }
          .map { case ((r, c), v) => (r, c, v) }
      }
    }
    
    override val sizeHint = left.sizeHint + right.sizeHint
    
    implicit override val rowOrd: Ordering[R] = left.rowOrd
    implicit override val colOrd: Ordering[C] = left.colOrd
  }

  case class Literal[R, C, V](override val toTypedPipe: TypedPipe[(R, C, V)], override val sizeHint: SizeHint)(implicit override val rowOrd: Ordering[R], override val colOrd: Ordering[C]) extends Matrix2[R, C, V]

object Matrix2 {
  
  /**
   * The original prototype that employs the standard O(n^3) dynamic programming
   * procedure to optimize a matrix chain factorization
   */
  def optimizeProductChain[V](p: IndexedSeq[Matrix2[Any, Any, V]])(implicit ring: Ring[V]): (Long, Matrix2[Any, Any, V]) = {

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
        val result = Product(left, right, true, ring)
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
   * + it now does "global" optimization - i.e. over optimize over basic blocks.
   * In the above example, we'd treat (D+E) as a temporary matrix T and optimize the whole chain ABCTFG
   * 
   * Not sure if making use of distributivity to generate more variants would be good.
   * In the above example, we could also generate ABCDFG + ABCEFG and have basic blocks: ABCDFG, and ABCEFG.
   * But this would be almost twice as much work with the current cost estimation.
   */
  def optimize[V](mf: Matrix2[Any, Any, V])(implicit ring: Ring[V]): (Long, Matrix2[Any, Any, V]) = {

    /**
     * Helper function that either returns an optimized product chain
     * or the last visited place in the tree
     */
    def chainOrLast(chain: List[Matrix2[Any, Any, V]], last: Option[(Long, Matrix2[Any, Any, V])]): (Long, Matrix2[Any, Any, V]) = {
      if (chain.isEmpty) last.get
      else optimizeProductChain(chain.toIndexedSeq)
    }

    /**
     * Recursive function - returns a flatten product chain so far and the rest of the connected tree
     */
    def optimizeBasicBlocks(mf: Matrix2[Any, Any, V]): (List[Matrix2[Any, Any, V]], Long, Option[(Long, Matrix2[Any, Any, V])]) = {
     mf match {
        // basic block of one matrix
        case element: Literal[Any, Any, V] => (List(element), 0, None)
        // two potential basic blocks connected by a sum
        case Sum(left, right, _) => {
          val (lastLChain, lastCost1, leftTemp) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, rightTemp) = optimizeBasicBlocks(right)
          val (cost1, newLeft) = chainOrLast(lastLChain, leftTemp)
          val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
          (List(Sum(newLeft, newRight, ring)), lastCost1 + lastCost2 + cost1 + cost2, None)
        }
        // basic block A*B
        case Product(leftp: Literal[Any, Any, V], rightp: Literal[Any, Any, V], _, _) => {
          (List(leftp, rightp), 0, None)
        }
        // potential chain (...something...)*right or just two basic blocks connected by a product
        case Product(left: Product[Any, Any, Any, V], right: Literal[Any, Any, V], _, _) => {
          val (lastLChain, lastCost, leftTemp) = optimizeBasicBlocks(left)
          if (lastLChain.isEmpty) {
            val (cost, newLeft) = leftTemp.get
            val interProduct = Product(newLeft, right, true, ring)
            (Nil, cost + lastCost, Some(cost, interProduct))
          } else {
            (lastLChain ++ List(right), lastCost, leftTemp)
          }
        }
        // potential chain left*(...something...) or just two basic blocks connected by a product
        case Product(left: Literal[Any, Any, V], right: Product[Any, Any, Any, V], _, _) => {
          val (lastRChain, lastCost, rightTemp) = optimizeBasicBlocks(right)
          if (lastRChain.isEmpty) {
            val (cost, newRight) = rightTemp.get
            val interProduct = Product(left, newRight, true, ring)
            (Nil, cost + lastCost, Some(cost, interProduct))
          } else {
            (left :: lastRChain, lastCost, rightTemp)
          }
        }
        // potential chain (...something...)*(...something...) or just two basic blocks connected by a product
        case Product(left, right, _, _) => {
          val (lastLChain, lastCost1, leftTemp) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, rightTemp) = optimizeBasicBlocks(right)
          if (lastLChain.isEmpty) {
            val (cost1, newLeft) = leftTemp.get
            val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
            (Nil, cost1 + cost2 + lastCost1 + lastCost2, Some(cost1 + cost2 + lastCost1 + lastCost2, Product(newLeft, newRight, true, ring)))
          } else {
            if (lastRChain.isEmpty) {
              val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq)
              val (cost2, newRight) = rightTemp.get
              (Nil, cost1 + cost2 + lastCost1 + lastCost2, Some(cost1 + cost2 + lastCost1 + lastCost2, Product(newLeft, newRight, true, ring)))
            } else {
              (lastLChain ++ lastRChain, lastCost1 + lastCost2, None)
            }
          }
        }
      }
    }
    val (lastChain, lastCost, form) = optimizeBasicBlocks(mf)
    val (potentialCost, finalResult) = chainOrLast(lastChain, form)
    (lastCost + potentialCost, finalResult) 
  }

}
