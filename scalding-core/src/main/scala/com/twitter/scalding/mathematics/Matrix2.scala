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
  def -(that: Matrix2[R, C, V])(implicit g: Group[V]): Matrix2[R, C, V] = Sum(this, that.negate, g)
  def negate(implicit g: Group[V]): Matrix2[R, C, V]
  // TODO: Hadamard product
  def #*#(that: Matrix2[R, C, V]): Matrix2[R, C, V] = sys.error("todo")
  // Matrix product
  def *[C2](that: Matrix2[C, C2, V])(implicit ring: Ring[V]): Matrix2[R, C2, V] = Product(this, that, false, ring)
  def toTypedPipe: TypedPipe[(R, C, V)]
  def transpose: Matrix2[C, R, V]
  def optimizedSelf: Matrix2[R, C, V] = Matrix2.optimize(this.asInstanceOf[Matrix2[Any, Any, V]])._2.asInstanceOf[Matrix2[R, C, V]]
  // TODO: complete the rest of the API to match the old Matrix API (many methods are effectively on the TypedPipe)
  def sumColVectors(implicit ring: Ring[V]): Matrix2[R, Unit, V] = Product(this, new OneC(colOrd), false, ring)
  
  def propagate[VecV](vec: Matrix2[C, Unit, VecV])(implicit ring: Ring[VecV])
    : Matrix2[R, Unit, VecV] = {
    //This cast will always succeed:
    val boolMat = this.asInstanceOf[Matrix2[R,C,Boolean]].toTypedPipe
    val one = boolMat.groupBy(x => x._2)
    val two = vec.toTypedPipe.groupBy(x => x._1)
    MatrixLiteral(one.join(two).mapValues { boolT => if (boolT._1._3) (boolT._1._1, boolT._1._2, boolT._2._3) else (boolT._1._1, boolT._1._2, ring.zero) }
    .values, this.sizeHint).sumColVectors
  }
  
  // Binarize values, all x != 0 become 1
  def binarizeAs[NewValT](implicit mon : Monoid[V], ring : Ring[NewValT]) : Matrix2[R,C,NewValT] = {
    val newPipe = this.toTypedPipe.map{ case (r, c, x) => (r, c, if ( mon.isNonZero(x) ) { ring.one } else { ring.zero }) }
    MatrixLiteral(newPipe, this.sizeHint)
  }  
}

/**
 * Infinite column vector - only for intermediate computations
 */
class OneC[C, V](override val rowOrd: Ordering[C]) extends Matrix2[C, Unit, V] {
  override val sizeHint: SizeHint = FiniteHint(Long.MaxValue, 1) 
  override def colOrd = Ordering[Unit]
  def transpose = sys.error("Only used in intermediate computations") // will be OneR
  override def negate(implicit g: Group[V]) = sys.error("Only used in intermediate computations")
  def toTypedPipe = sys.error("Only used in intermediate computations")
}

case class Product[R, C, C2, V](left: Matrix2[R, C, V], right: Matrix2[C, C2, V], optimal: Boolean = false, ring: Ring[V]) extends Matrix2[R, C2, V] {

  def toTypedPipe: TypedPipe[(R, C2, V)] = {
    if (optimal) {
      if (right.isInstanceOf[OneC[C, V]]) {
        val ord: Ordering[R] = left.rowOrd
        left.toTypedPipe.groupBy(x => x._1)(ord).mapValues { _._3 }
        	.sum(ring)
	        .filter { kv => ring.isNonZero(kv._2) }
	        .map { case (r, v) => (r, (), v) }.asInstanceOf[TypedPipe[(R, C2, V)]] // we know C2 is Unit         
      } else {
	      val ord: Ordering[C] = left.colOrd
	      val ord2: Ordering[(R, C2)] = Ordering.Tuple2(rowOrd, colOrd)
	      val maxRatio = 10000L
	      val one = left.toTypedPipe.groupBy(x => x._2)(ord)
	      val two = right.toTypedPipe.groupBy(x => x._1)(ord)
	      val sizeOne = left.sizeHint.total.getOrElse(1L)
	      val sizeTwo = right.sizeHint.total.getOrElse(1L)
	      val joined = if (sizeOne / sizeTwo > maxRatio) {
	        one.hashJoin(two).map { case (key, ((l1, l2, lv), (r1, r2, rv))) => (l1, r2, ring.times(lv, rv)) }
	      } else if (sizeTwo / sizeOne > maxRatio) {
	        two.hashJoin(one).map { case (key, ((l1, l2, lv), (r1, r2, rv))) => (r1, l2, ring.times(lv, rv)) }
	      } else if (sizeOne > sizeTwo) {
	        one.join(two).mapValues { case (l, r) => (l._1, r._2, ring.times(l._3, r._3)) }.values
	      } else {
	        two.join(one).mapValues { case (l, r) => (r._1, l._2, ring.times(l._3, r._3)) }.values
	      }
	      joined.groupBy(w => (w._1, w._2))(ord2).mapValues { _._3 }
	        .sum(ring)
	        .filter { kv => ring.isNonZero(kv._2) }
	        .map { case ((r, c), v) => (r, c, v) }
      }
    } else {
      optimizedSelf.toTypedPipe
    }
  }

  override val sizeHint = left.sizeHint * right.sizeHint

  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C2] = right.colOrd
  override lazy val transpose: Product[C2, C, R, V] = Product(right.transpose, left.transpose, false, ring)
  override def negate(implicit g: Group[V]): Product[R, C, C2, V] = if (left.sizeHint.total.getOrElse(0L) > right.sizeHint.total.getOrElse(0L)) Product(left, right.negate, optimal, ring) else Product(left.negate, right, optimal, ring)
}

case class Sum[R, C, V](left: Matrix2[R, C, V], right: Matrix2[R, C, V], mon: Monoid[V]) extends Matrix2[R, C, V] {
  def collectAddends(sum: Sum[R, C, V]): List[Either[Product[R, _, C, V], MatrixLiteral[R, C, V]]] = {
    def eitherWrapper(mat: Matrix2[R, C, V]): Either[Product[R, _, C, V], MatrixLiteral[R, C, V]] = {
      mat match {
        case x @ Product(_, _, _, _) => Left(x)
        case x @ MatrixLiteral(_, _) => Right(x)
        case _ => sys.error("Invalid addend")
      }
    }

    sum match {
      case Sum(l @ Sum(_, _, _), r @ Sum(_, _, _), _) => {
        collectAddends(l) ++ collectAddends(r)
      }
      case Sum(l @ Sum(_, _, _), r, _) => {
        collectAddends(l) ++ List(eitherWrapper(r))
      }
      case Sum(l, r @ Sum(_, _, _), _) => {
        eitherWrapper(l) :: collectAddends(r)
      }
      case Sum(l, r, _) => {
        List(eitherWrapper(l), eitherWrapper(r))
      }
    }
  }

  def toTypedPipe: TypedPipe[(R, C, V)] = {
    if (left.equals(right)) {
      left.optimizedSelf.toTypedPipe.map(v => (v._1, v._2, mon.plus(v._3, v._3)))
    } else {
      val ord: Ordering[(R, C)] = Ordering.Tuple2(left.rowOrd, left.colOrd)
      val toAdd = {
        val addends = collectAddends(this)
        addends.map(x => x match {
          // x is never a Sum, i.e. toTypedPipe call does not recurse
          case Left(addend) => addend.optimizedSelf.toTypedPipe
          case Right(addend) => addend.toTypedPipe
        }).reduce((x, y) => x ++ y)
      }
      toAdd
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
  override def negate(implicit g: Group[V]): Sum[R, C, V] = Sum(left.negate, right.negate, mon)
}

case class MatrixLiteral[R, C, V](override val toTypedPipe: TypedPipe[(R, C, V)], override val sizeHint: SizeHint)(implicit override val rowOrd: Ordering[R], override val colOrd: Ordering[C]) extends Matrix2[R, C, V] {
  override lazy val transpose: MatrixLiteral[C, R, V] = MatrixLiteral(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)(colOrd, rowOrd)
  override def negate(implicit g: Group[V]): MatrixLiteral[R, C, V] = MatrixLiteral(toTypedPipe.map(x => (x._1, x._2, g.negate(x._3))), sizeHint)
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
        case element@MatrixLiteral(_, _) => (List(element), 0, None)
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
        case el => (List(el), 0, None) 
      }
    }
    val (lastChain, lastCost, ring) = optimizeBasicBlocks(mf)
    val (potentialCost, finalResult) = optimizeProductChain(lastChain.toIndexedSeq, ring)
    (lastCost + potentialCost, finalResult)
  }

}
