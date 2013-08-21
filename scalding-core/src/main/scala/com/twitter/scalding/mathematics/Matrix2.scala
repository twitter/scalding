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
import scala.collection.mutable.Map
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
  // Hadamard product
  def #*#(that: Matrix2[R, C, V])(implicit ring: Ring[V]): Matrix2[R, C, V] = HadamardProduct(this, that, ring)
  // Matrix product
  def *[C2](that: Matrix2[C, C2, V])(implicit ring: Ring[V]): Matrix2[R, C2, V] = Product(this, that, false, ring)
  def toTypedPipe: TypedPipe[(R, C, V)]
  def transpose: Matrix2[C, R, V]
  def optimizedSelf: Matrix2[R, C, V] = Matrix2.optimize(this.asInstanceOf[Matrix2[Any, Any, V]])._2.asInstanceOf[Matrix2[R, C, V]]
  // TODO: complete the rest of the API to match the old Matrix API (many methods are effectively on the TypedPipe)
  def sumColVectors(implicit ring: Ring[V]): Matrix2[R, Unit, V] = Product(this, OneC()(colOrd), false, ring)

  def joinWith[C2, V2](that: Matrix2[C, C2, V2], maxRatio: Long = 10000L): Either[TypedPipe[(C, ((R, C, V), (C, C2, V2)))], TypedPipe[(C, ((C, C2, V2), (R, C, V)))]] = {
    val one = this.toTypedPipe.groupBy(x => x._2)
    val two = that.toTypedPipe.groupBy(x => x._1)
    val sizeOne = this.sizeHint.total.getOrElse(BigInt(1L))
    val sizeTwo = that.sizeHint.total.getOrElse(BigInt(1L))
    if (sizeOne / sizeTwo > maxRatio) {
      Left(one.hashJoin(two))
    } else if (sizeTwo / sizeOne > maxRatio) {
      Right(two.hashJoin(one))
    } else if (sizeOne > sizeTwo) {
      Left(one.join(two).toTypedPipe)
    } else {
      Right(two.join(one).toTypedPipe)
    }
  }

  def propagate[VecV](vec: Matrix2[C, Unit, VecV])(implicit ev: =:=[V, Boolean], mon: Monoid[VecV]): Matrix2[R, Unit, VecV] = {
    //This cast will always succeed:
    lazy val joinedBool = this.asInstanceOf[Matrix2[R, C, Boolean]].joinWith(vec)
    lazy val resultPipe = (joinedBool match {
      case Left(x) => x.map {
        case (key, boolT) =>
          if (boolT._1._3) (boolT._1._1, boolT._1._2, boolT._2._3)
          else (boolT._1._1, boolT._1._2, mon.zero)
      }
      case Right(x) => x.map {
        case (key, boolT) =>
          if (boolT._2._3) (boolT._2._1, boolT._2._2, boolT._1._3)
          else (boolT._2._1, boolT._2._2, mon.zero)
      }
    }).groupBy(w => (w._1))(rowOrd)
      .mapValues { _._3 }
      .sum(mon)
      .filter { kv => mon.isNonZero(kv._2) }
      .map { case (r, v) => (r, (), v) }
    MatrixLiteral(resultPipe, this.sizeHint)
  }

  def propagateRow[C2](mat: Matrix2[C, C2, Boolean])(implicit ev: =:=[R, Unit], mon: Monoid[V]): Matrix2[Unit, C2, V] = {
    mat.transpose.propagate(this.transpose.asInstanceOf[Matrix2[C, Unit, V]]).transpose
  }

  // Binarize values, all x != 0 become 1
  def binarizeAs[NewValT](implicit mon: Monoid[V], ring: Ring[NewValT]): Matrix2[R, C, NewValT] = {
    lazy val newPipe = this.toTypedPipe.map { case (r, c, x) => (r, c, if (mon.isNonZero(x)) { ring.one } else { ring.zero }) }.filter { kv => ring.isNonZero(kv._3) }
    MatrixLiteral(newPipe, this.sizeHint)
  }

  // Row L2 normalization - as in the old API, but diagonal and inverse included in it in the map
  protected lazy val rowL2Norm = {
    val matD = this.asInstanceOf[Matrix2[R, C, Double]]
    lazy val result = MatrixLiteral(matD.toTypedPipe.map { case (r, c, x) => (r, c, x * x) }, this.sizeHint).sumColVectors.
      toTypedPipe.map { case (r, c, x) => (r, r, 1 / scala.math.sqrt(x)) } // diagonal + inverse
    MatrixLiteral(result, SizeHint.asDiagonal(this.sizeHint.setRowsToCols)) * matD
  }

  // Row L2 normalization (can only be called for Double)
  // After this operation, the sum(|x|^2) along each row will be 1. 
  def rowL2Normalize(implicit ev: =:=[V, Double]): Matrix2[R, C, Double] = rowL2Norm

  def getRow(index: R): Matrix2[Unit, C, V] = MatrixLiteral(toTypedPipe.filter { case (r, c, v) => Ordering[R].equiv(r, index) }.map { case (r, c, v) => ((), c, v) }, this.sizeHint.setRows(1L))
  def getColumn(index: C): Matrix2[R, Unit, V] = MatrixLiteral(toTypedPipe.filter { case (r, c, v) => Ordering[C].equiv(c, index) }.map { case (r, c, v) => (r, (), v) }, this.sizeHint.setCols(1L))
}

/**
 * Infinite column vector - only for intermediate computations
 */
case class OneC[R, V](implicit override val rowOrd: Ordering[R]) extends Matrix2[R, Unit, V] {
  override val sizeHint: SizeHint = FiniteHint(Long.MaxValue, 1)
  override def colOrd = Ordering[Unit]
  def transpose = OneR()
  override def negate(implicit g: Group[V]) = sys.error("Only used in intermediate computations")
  def toTypedPipe = sys.error("Only used in intermediate computations")
}

/**
 * Infinite row vector - only for intermediate computations
 */
case class OneR[C, V](implicit override val colOrd: Ordering[C]) extends Matrix2[Unit, C, V] {
  override val sizeHint: SizeHint = FiniteHint(1, Long.MaxValue)
  override def rowOrd = Ordering[Unit]
  def transpose = OneC()
  override def negate(implicit g: Group[V]) = sys.error("Only used in intermediate computations")
  def toTypedPipe = sys.error("Only used in intermediate computations")
}

/**
 * Class representing a matrix product
 * 
 * @param left multiplicand
 * @param right multiplier
 * @param optimal whether this Product went through the optimize function 
 * @param expressions a HashMap of common subtrees; None if optimal is false, Some(...) with a HashMap that was created in optimize
 */
case class Product[R, C, C2, V](left: Matrix2[R, C, V], right: Matrix2[C, C2, V], optimal: Boolean = false, ring: Ring[V], expressions: Option[Map[Matrix2[R, C2, V], TypedPipe[(R, C2, V)]]] = None) extends Matrix2[R, C2, V] {

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[Product[_, _, _, _]]) {
      val product = obj.asInstanceOf[Product[R, C, C2, V]]
      product.left.equals(left) && product.right.equals(right)
    } else {
      false
    }
  }

  override def hashCode(): Int = left.hashCode ^ right.hashCode

  private lazy val isSpecialCase: Boolean = right.isInstanceOf[OneC[_, _]] || left.isInstanceOf[OneR[_, _]]

  private lazy val specialCase: TypedPipe[(R, C2, V)] = {
    val leftMatrix = right.isInstanceOf[OneC[_, _]]
    val joined = (if (leftMatrix) {
      val ord: Ordering[R] = left.rowOrd
      left.toTypedPipe.groupBy(x => x._1)(ord)
    } else {
      val ord: Ordering[C] = right.rowOrd
      right.toTypedPipe.groupBy(x => x._1)(ord)
    }).mapValues { _._3 }
      .sum(ring)
      .filter { kv => ring.isNonZero(kv._2) }

    if (leftMatrix) {
      joined.map { case (r, v) => (r, (), v) }.asInstanceOf[TypedPipe[(R, C2, V)]] // we know C2 is Unit
    } else {
      joined.map { case (c, v) => ((), c, v) }.asInstanceOf[TypedPipe[(R, C2, V)]] // we know R is Unit 
    }
  }

  // represents `\sum_{i j} M_{i j}` where `M_{i j}` is the Matrix with exactly one element at `row=i, col = j`.
  lazy val toOuterSum: TypedPipe[(R, C2, V)] = {
    if (optimal) {
      if (isSpecialCase) {
        specialCase
      } else {
        val ord: Ordering[C] = left.colOrd
        val joined = left.joinWith(right)
        joined match {
          case Left(x) => x.map { case (key, ((l1, l2, lv), (r1, r2, rv))) => (l1, r2, ring.times(lv, rv)) }
          case Right(x) => x.map { case (key, ((l1, l2, lv), (r1, r2, rv))) => (r1, l2, ring.times(lv, rv)) }
        }
      }
    } else {
      // this branch might be tricky, since not clear to me that optimizedSelf will be a Product with a known C type
      // Maybe it is Product[R, _, C2, V]
      optimizedSelf.asInstanceOf[Product[R, _, C2, V]].toOuterSum
    }
  }

  private lazy val computePipe: TypedPipe[(R, C2, V)] = {
    val joined = toOuterSum
    val result = if (isSpecialCase) {
      joined
    } else {
      val ord2: Ordering[(R, C2)] = Ordering.Tuple2(rowOrd, colOrd)
      joined.groupBy(w => (w._1, w._2))(ord2).mapValues { _._3 }
        .sum(ring)
        .filter { kv => ring.isNonZero(kv._2) }
        .map { case ((r, c), v) => (r, c, v) }
    }
    if (expressions.isDefined) {
      expressions.get.put(this, result)
    }
    result
  }

  override lazy val toTypedPipe: TypedPipe[(R, C2, V)] = {
    if (optimal) {
      expressions match {
        case Some(m) => m.get(this) match {
          case Some(pipe) => pipe
          case None => computePipe
        }
        case None => computePipe
      }
    } else {
      optimizedSelf.toTypedPipe
    }

  }

  override val sizeHint = left.sizeHint * right.sizeHint

  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C2] = right.colOrd
  override lazy val transpose: Product[C2, C, R, V] = Product(right.transpose, left.transpose, false, ring)
  override def negate(implicit g: Group[V]): Product[R, C, C2, V] = if (left.sizeHint.total.getOrElse(BigInt(0L)) > right.sizeHint.total.getOrElse(BigInt(0L))) Product(left, right.negate, optimal, ring, expressions) else Product(left.negate, right, optimal, ring, expressions)
}

case class Sum[R, C, V](left: Matrix2[R, C, V], right: Matrix2[R, C, V], mon: Monoid[V]) extends Matrix2[R, C, V] {
  def collectAddends(sum: Sum[R, C, V]): List[TypedPipe[(R, C, V)]] = {
    def getLiteral(mat: Matrix2[R, C, V]): TypedPipe[(R, C, V)] = {
      mat match {
        case x @ Product(_, _, _, _, _) => x.toOuterSum
        case x @ MatrixLiteral(_, _) => x.toTypedPipe
        case x @ HadamardProduct(_, _, _) => x.optimizedSelf.toTypedPipe
        case _ => sys.error("Invalid addend")
      }
    }

    sum match {
      case Sum(l @ Sum(_, _, _), r @ Sum(_, _, _), _) => {
        collectAddends(l) ++ collectAddends(r)
      }
      case Sum(l @ Sum(_, _, _), r, _) => {
        collectAddends(l) ++ List(getLiteral(r))
      }
      case Sum(l, r @ Sum(_, _, _), _) => {
        getLiteral(l) :: collectAddends(r)
      }
      case Sum(l, r, _) => {
        List(getLiteral(l), getLiteral(r))
      }
    }
  }

  override lazy val toTypedPipe: TypedPipe[(R, C, V)] = {
    if (left.equals(right)) {
      left.optimizedSelf.toTypedPipe.map(v => (v._1, v._2, mon.plus(v._3, v._3)))
    } else {
      val ord: Ordering[(R, C)] = Ordering.Tuple2(left.rowOrd, left.colOrd)
      collectAddends(this)
        .reduce((x, y) => x ++ y)
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
  override def sumColVectors(implicit ring: Ring[V]): Matrix2[R, Unit, V] = Sum(left.sumColVectors, right.sumColVectors, mon)
}

case class HadamardProduct[R, C, V](left: Matrix2[R, C, V], right: Matrix2[R, C, V], ring: Ring[V]) extends Matrix2[R, C, V] {
  // TODO: optimize / combine with Sums: https://github.com/tomtau/scalding/issues/14#issuecomment-22971582
  override lazy val toTypedPipe: TypedPipe[(R, C, V)] = {
    if (left.equals(right)) {
      left.optimizedSelf.toTypedPipe.map(v => (v._1, v._2, ring.times(v._3, v._3)))
    } else {
      val ord: Ordering[(R, C)] = Ordering.Tuple2(left.rowOrd, left.colOrd)
      // tracking values which were reduced (multiplied by non-zero) or non-reduced (multiplied by zero) with a boolean
      val joined = left.optimizedSelf.toTypedPipe.map { case (r, c, v) => (r, c, (v, false)) } ++ right.optimizedSelf.toTypedPipe.map { case (r, c, v) => (r, c, (v, false)) }
      joined
        .groupBy(x => (x._1, x._2))(ord)
        .mapValues { _._3 }
        .reduce((x, y) => (ring.times(x._1, y._1), true))
        .filter { kv => kv._2._2 && ring.isNonZero(kv._2._1) }
        .map { case ((r, c), v) => (r, c, v._1) }
    }
  }

  override lazy val transpose: MatrixLiteral[C, R, V] = MatrixLiteral(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)(colOrd, rowOrd)
  override val sizeHint = left.sizeHint + right.sizeHint
  override def negate(implicit g: Group[V]): HadamardProduct[R, C, V] = if (left.sizeHint.total.getOrElse(BigInt(0L)) > right.sizeHint.total.getOrElse(BigInt(0L))) HadamardProduct(left, right.negate, ring) else HadamardProduct(left.negate, right, ring)
  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C] = left.colOrd
}

case class MatrixLiteral[R, C, V](override val toTypedPipe: TypedPipe[(R, C, V)], override val sizeHint: SizeHint)(implicit override val rowOrd: Ordering[R], override val colOrd: Ordering[C]) extends Matrix2[R, C, V] {
  override lazy val transpose: MatrixLiteral[C, R, V] = MatrixLiteral(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)(colOrd, rowOrd)
  override def negate(implicit g: Group[V]): MatrixLiteral[R, C, V] = MatrixLiteral(toTypedPipe.map(x => (x._1, x._2, g.negate(x._3))), sizeHint)
}

object Matrix2 {

  def J[R, C, V](implicit ordR: Ordering[R], ordC: Ordering[C], ring: Ring[V]) = Product(OneC[R, V]()(ordR), OneR[C, V]()(ordC), false, ring)

  /**
   * The original prototype that employs the standard O(n^3) dynamic programming
   * procedure to optimize a matrix chain factorization
   */
  def optimizeProductChain[V](p: IndexedSeq[Matrix2[Any, Any, V]], ring: Option[Ring[V]]): (BigInt, Matrix2[Any, Any, V]) = {

    val subchainCosts = HashMap.empty[(Int, Int), BigInt]

    val splitMarkers = HashMap.empty[(Int, Int), Int]

    def computeCosts(p: IndexedSeq[Matrix2[Any, Any, V]], i: Int, j: Int): BigInt = {
      if (subchainCosts.contains((i, j))) subchainCosts((i, j))
      if (i == j) subchainCosts.put((i, j), 0)
      else {
        subchainCosts.put((i, j), Long.MaxValue)
        for (k <- i to (j - 1)) {
          val cost = computeCosts(p, i, k) + computeCosts(p, k + 1, j) +
            (p(i).sizeHint * (p(k).sizeHint * p(j).sizeHint)).total.getOrElse(BigInt(0L))
          if (cost < subchainCosts((i, j))) {
            subchainCosts.put((i, j), cost)
            splitMarkers.put((i, j), k)
          }
        }
      }

      subchainCosts((i, j))
    }

    val sharedMap = HashMap.empty[Matrix2[Any, Any, V], TypedPipe[(Any, Any, V)]]

    def generatePlan(i: Int, j: Int): Matrix2[Any, Any, V] = {
      if (i == j) p(i)
      else {
        val k = splitMarkers((i, j))
        val left = generatePlan(i, k)
        val right = generatePlan(k + 1, j)
        Product(left, right, true, ring.get, Some(sharedMap))
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
  def optimize[V](mf: Matrix2[Any, Any, V]): (BigInt, Matrix2[Any, Any, V]) = {

    /**
     * Recursive function - returns a flatten product chain and optimizes product chains under sums
     */
    def optimizeBasicBlocks(mf: Matrix2[Any, Any, V]): (List[Matrix2[Any, Any, V]], BigInt, Option[Ring[V]]) = {
      mf match {
        // basic block of one matrix
        case element @ MatrixLiteral(_, _) => (List(element), 0, None)
        // two potential basic blocks connected by a sum
        case Sum(left, right, mon) => {
          val (lastLChain, lastCost1, ringL) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, ringR) = optimizeBasicBlocks(right)
          val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq, ringL)
          val (cost2, newRight) = optimizeProductChain(lastRChain.toIndexedSeq, ringR)
          (List(Sum(newLeft, newRight, mon)), lastCost1 + lastCost2 + cost1 + cost2, ringL.orElse(ringR))
        }
        case HadamardProduct(left, right, ring) => {
          val (lastLChain, lastCost1, ringL) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, ringR) = optimizeBasicBlocks(right)
          val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq, ringL)
          val (cost2, newRight) = optimizeProductChain(lastRChain.toIndexedSeq, ringR)
          (List(HadamardProduct(newLeft, newRight, ring)), lastCost1 + lastCost2 + cost1 + cost2, ringL.orElse(ringR))
        }
        // chain (...something...)*(...something...)
        case Product(left, right, _, ring, _) => {
          val (lastLChain, lastCost1, ringL) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, ringR) = optimizeBasicBlocks(right)
          (lastLChain ++ lastRChain, lastCost1 + lastCost2, Some(ring))
        }
        // OneC, OneR and potentially other intermediate matrices
        case el => (List(el), 0, None)
      }
    }
    val (lastChain, lastCost, ring) = optimizeBasicBlocks(mf)
    val (potentialCost, finalResult) = optimizeProductChain(lastChain.toIndexedSeq, ring)
    (lastCost + potentialCost, finalResult)
  }

}
