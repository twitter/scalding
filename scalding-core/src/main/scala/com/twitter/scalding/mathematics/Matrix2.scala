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
import com.twitter.algebird.{ Monoid, Ring, Group, Field }
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import cascading.flow.FlowDef

import java.io.Serializable

/**
 * *************
 * * WARNING: This is a new experimental API. Some features are missing
 * * and expect it to break. Use the old Matrix API if you do not feel adventurous.
 * **************
 */
sealed trait Matrix2[R, C, V] extends Serializable {
  implicit def rowOrd: Ordering[R]
  implicit def colOrd: Ordering[C]
  val sizeHint: SizeHint = NoClue
  def +(that: Matrix2[R, C, V])(implicit mon: Monoid[V]): Matrix2[R, C, V] = Sum(this, that, mon)
  def -(that: Matrix2[R, C, V])(implicit g: Group[V]): Matrix2[R, C, V] = Sum(this, that.negate, g)
  def unary_-(implicit g: Group[V]): Matrix2[R, C, V] = negate
  def negate(implicit g: Group[V]): Matrix2[R, C, V]
  // Hadamard product
  def #*#(that: Matrix2[R, C, V])(implicit ring: Ring[V]): Matrix2[R, C, V] = HadamardProduct(this, that, ring)
  // Matrix product
  def *[C2](that: Matrix2[C, C2, V])(implicit ring: Ring[V]): Matrix2[R, C2, V] = Product(this, that, ring)
  def *(that: Scalar2[V])(implicit ring: Ring[V], mode: Mode, flowDef: FlowDef): Matrix2[R, C, V] = that * this
  def /(that: Scalar2[V])(implicit field: Field[V], mode: Mode, flowDef: FlowDef): Matrix2[R, C, V] =
    that divMatrix this
  def toTypedPipe: TypedPipe[(R, C, V)]
  def transpose: Matrix2[C, R, V]
  def optimizedSelf: Matrix2[R, C, V] =
    Matrix2.optimize(this.asInstanceOf[Matrix2[Any, Any, V]])._2.asInstanceOf[Matrix2[R, C, V]]

  def ^(power: Int)(implicit ev: =:=[R, C], ring: Ring[V]): Matrix2[R, R, V] = {
    // it can possibly be pre-computed in an optimal way as g^k = ((g*g)*(g*g)...
    // but it is handled in "optimize" in general, so that (g^k)*something works
    assert(power > 0, "exponent must be >= 1")
    val literal = this.asInstanceOf[Matrix2[R, R, V]]
    if (power == 1) {
      literal
    } else {
      literal * (literal ^ (power - 1))
    }
  }

  // TODO: complete the rest of the API to match the old Matrix API (many methods are effectively on the TypedPipe)
  def sumColVectors(implicit ring: Ring[V]): Matrix2[R, Unit, V] = Product(this, OneC()(colOrd), ring)

  def joinWith[C2, V2](that: Matrix2[C, C2, V2], maxRatio: Long = 10000L): TypedPipe[(C, ((R, C, V), (C, C2, V2)))] = {
    val one = this.toTypedPipe.groupBy(x => x._2)
    val two = that.toTypedPipe.groupBy(x => x._1)
    val sizeOne = this.sizeHint.total.getOrElse(BigInt(1L))
    val sizeTwo = that.sizeHint.total.getOrElse(BigInt(1L))

    def swapInner[M,N](t: TypedPipe[(C, (M, N))]): TypedPipe[(C, (N, M))] = t.mapValues { t: (M,N) => t.swap }

    if (sizeOne / sizeTwo > maxRatio) {
      one.hashJoin(two)
    } else if (sizeTwo / sizeOne > maxRatio) {
      swapInner(two.hashJoin(one))
    } else if (sizeOne > sizeTwo) {
      one.join(two).toTypedPipe
    } else {
      swapInner(two.join(one).toTypedPipe)
    }
  }

  // TODO This is a special kind of product that could be optimized like Product is
  def propagate[C2, VecV](vec: Matrix2[C, C2, VecV])(implicit ev: =:=[V, Boolean], mon: Monoid[VecV]): Matrix2[R, C2, VecV] = {
    //This cast will always succeed:
    lazy val joinedBool = this.asInstanceOf[Matrix2[R, C, Boolean]].joinWith(vec)
    implicit val ord2: Ordering[C2] = vec.colOrd
    lazy val resultPipe = joinedBool.flatMap { case (key, ((row, _, bool), (_, col2, v))) =>
        if (bool) Some((row, col2), v) else None // filter early
      }
      .group // TODO we could be lazy with this group and combine with a sum
      .sum
      .filter { kv => mon.isNonZero(kv._2) }
      .map { case ((r, c2), v) => (r, c2, v) }
    MatrixLiteral(resultPipe, this.sizeHint)
  }

  def propagateRow[C2](mat: Matrix2[C, C2, Boolean])(implicit ev: =:=[R, Unit], mon: Monoid[V]): Matrix2[Unit, C2, V] =
    mat.transpose.propagate(this.transpose.asInstanceOf[Matrix2[C, Unit, V]]).transpose

  // Binarize values, all x != 0 become 1
  def binarizeAs[NewValT](implicit mon: Monoid[V], ring: Ring[NewValT]): Matrix2[R, C, NewValT] = {
    lazy val newPipe = toTypedPipe.map { case (r, c, x) =>
        (r, c, if (mon.isNonZero(x)) { ring.one } else { ring.zero })
      }
      .filter { kv => ring.isNonZero(kv._3) }
    MatrixLiteral(newPipe, this.sizeHint)
  }

  // Row L2 normalization - as in the old API, but diagonal and inverse included in it in the map
  protected lazy val rowL2Norm = {
    val matD = this.asInstanceOf[Matrix2[R, C, Double]]
    lazy val result = MatrixLiteral(matD.toTypedPipe.map { case (r, c, x) => (r, c, x * x) }, this.sizeHint)
        .sumColVectors
        .toTypedPipe
        .map { case (r, c, x) => (r, r, 1 / scala.math.sqrt(x)) } // diagonal + inverse
    MatrixLiteral(result, SizeHint.asDiagonal(this.sizeHint.setRowsToCols)) * matD
  }

  // Row L2 normalization (can only be called for Double)
  // After this operation, the sum(|x|^2) along each row will be 1.
  def rowL2Normalize(implicit ev: =:=[V, Double]): Matrix2[R, C, Double] = rowL2Norm

  def getRow(index: R): Matrix2[Unit, C, V] =
    MatrixLiteral(
      toTypedPipe
        .filter { case (r, c, v) => Ordering[R].equiv(r, index) }
        .map { case (r, c, v) => ((), c, v) }, this.sizeHint.setRows(1L)
      )

  def getColumn(index: C): Matrix2[R, Unit, V] =
    MatrixLiteral(
      toTypedPipe
        .filter { case (r, c, v) => Ordering[C].equiv(c, index) }
        .map { case (r, c, v) => (r, (), v) }, this.sizeHint.setCols(1L)
      )

  def asRow[R2](r2: R2)(implicit ev: R =:= Unit, rowOrd: Ordering[R2]): Matrix2[R2, C, V] =
    MatrixLiteral(toTypedPipe.map { case (r, c, v) => (r2, c, v) }, this.sizeHint)

  def asCol[C2](c2: C2)(implicit ev: C =:= Unit, colOrd: Ordering[C2]): Matrix2[R, C2, V] =
    MatrixLiteral(toTypedPipe.map { case (r, c, v) => (r, c2, v) }, this.sizeHint)

  // Compute the sum of the main diagonal.  Only makes sense cases where the row and col type are
  // equal
  def trace(implicit mon: Monoid[V], ev: =:=[R,C]): Scalar2[V] =
    ComputedScalar(toTypedPipe.asInstanceOf[TypedPipe[(R, R, V)]]
      .filter{case (r1, r2, _) => Ordering[R].equiv(r1, r2)}
      .map{case (_,_,x) => x}
      .sum(mon)
    )

  def write(sink: TypedSink[(R, C, V)])(implicit fd: FlowDef, m: Mode): Matrix2[R, C, V] =
    MatrixLiteral(toTypedPipe.write(sink), sizeHint)
}

/**
 * Infinite column vector - only for intermediate computations
 */
case class OneC[R, V](implicit override val rowOrd: Ordering[R]) extends Matrix2[R, Unit, V] {
  override val sizeHint: SizeHint = FiniteHint(Long.MaxValue, 1)
  override def colOrd = Ordering[Unit]
  def transpose = OneR()
  override def negate(implicit g: Group[V]) = sys.error("Only used in intermediate computations, try (-1 * OneC)")
  def toTypedPipe = sys.error("Only used in intermediate computations")
}

/**
 * Infinite row vector - only for intermediate computations
 */
case class OneR[C, V](implicit override val colOrd: Ordering[C]) extends Matrix2[Unit, C, V] {
  override val sizeHint: SizeHint = FiniteHint(1, Long.MaxValue)
  override def rowOrd = Ordering[Unit]
  def transpose = OneC()
  override def negate(implicit g: Group[V]) = sys.error("Only used in intermediate computations, try (-1 * OneR)")
  def toTypedPipe = sys.error("Only used in intermediate computations")
}

/**
 * Class representing a matrix product
 *
 * @param left multiplicand
 * @param right multiplier
 * @param ring
 * @param expressions a HashMap of common subtrees; None if possibly not optimal (did not go through optimize), Some(...) with a HashMap that was created in optimize
 */
case class Product[R, C, C2, V](left: Matrix2[R, C, V],
  right: Matrix2[C, C2, V],
  ring: Ring[V],
  expressions: Option[Map[Matrix2[R, C2, V], TypedPipe[(R, C2, V)]]] = None) extends Matrix2[R, C2, V] {

  /**
   * Structural, NOT mathematical equality (e.g. (A*B) * C != A * (B*C))
   * Used for the Matrix2OptimizationTest (so that it doesn't care about expressions)
   */
  override def equals(obj: Any): Boolean = obj match {
    case Product(tl, tr, _, _) => left.equals(tl) && right.equals(tr)
    case _ => false
  }

  override def hashCode(): Int = left.hashCode ^ right.hashCode

  private lazy val optimal: Boolean = expressions.isDefined

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
        left.joinWith(right)
          .map { case (key, ((l1, l2, lv), (r1, r2, rv))) => (l1, r2, ring.times(lv, rv)) }
      }
    } else {
      // this branch might be tricky, since not clear to me that optimizedSelf will be a Product with a known C type
      // Maybe it is Product[R, _, C2, V]
      optimizedSelf.asInstanceOf[Product[R, _, C2, V]].toOuterSum
    }
  }

  private def computePipe(joined: TypedPipe[(R, C2, V)] = toOuterSum): TypedPipe[(R, C2, V)] = {
    if (isSpecialCase) {
      joined
    } else {
      val ord2: Ordering[(R, C2)] = Ordering.Tuple2(rowOrd, colOrd)
      joined.groupBy(w => (w._1, w._2))(ord2).mapValues { _._3 }
        .sum(ring)
        .filter { kv => ring.isNonZero(kv._2) }
        .map { case ((r, c), v) => (r, c, v) }
    }
  }

  override lazy val toTypedPipe: TypedPipe[(R, C2, V)] = {
    expressions match {
      case Some(m) => m.get(this) match {
        case Some(pipe) => pipe
        case None => {
          val result = computePipe()
          m.put(this, result)
          result
        }
      }
      case None => optimizedSelf.toTypedPipe
    }
  }

  override val sizeHint = left.sizeHint * right.sizeHint

  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C2] = right.colOrd
  override lazy val transpose: Product[C2, C, R, V] = Product(right.transpose, left.transpose, ring)
  override def negate(implicit g: Group[V]): Product[R, C, C2, V] = {
    if (left.sizeHint.total.getOrElse(BigInt(0L)) > right.sizeHint.total.getOrElse(BigInt(0L))) {
      Product(left, right.negate, ring, expressions)
    } else {
      Product(left.negate, right, ring, expressions)
    }
  }

  // Trace(A B) = Trace(B A)
  override def trace(implicit mon: Monoid[V], ev1: =:=[R,C2]): Scalar2[V] = {
    val (cost1, plan1) = Matrix2.optimize(this.asInstanceOf[Matrix2[Any, Any, V]])
    val (cost2, plan2) = Matrix2.optimize(
      Product(right.asInstanceOf[Matrix2[C,R,V]], left.asInstanceOf[Matrix2[R,C,V]], ring, None)
        .asInstanceOf[Matrix2[Any, Any, V]]
      )

    if (cost1 > cost2) {
      val product2 = plan2.asInstanceOf[Product[C, R, C, V]]
      val ord = left.colOrd
      val filtered = product2.toOuterSum.filter{case (c1, c2, _) => ord.equiv(c1, c2)}
      ComputedScalar(product2.computePipe(filtered).map{case (_, _, x) => x}.sum(mon))
    } else {
      val product1 = plan1.asInstanceOf[Product[R, C, R, V]]
      val ord = left.rowOrd
      val filtered = product1.toOuterSum.filter{case (r1, r2, _) => ord.equiv(r1, r2)}
      ComputedScalar(product1.computePipe(filtered).map{case (_, _, x) => x}.sum(mon))
    }

  }
}

case class Sum[R, C, V](left: Matrix2[R, C, V], right: Matrix2[R, C, V], mon: Monoid[V]) extends Matrix2[R, C, V] {
  def collectAddends(sum: Sum[R, C, V]): List[TypedPipe[(R, C, V)]] = {
    def getLiteral(mat: Matrix2[R, C, V]): TypedPipe[(R, C, V)] = {
      mat match {
        case x @ Product(_, _, _, _) => x.toOuterSum
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

  override def trace(implicit mon: Monoid[V], ev: =:=[R,C]): Scalar2[V] =
    ComputedScalar(collectAddends(this).map { pipe =>
      pipe.asInstanceOf[TypedPipe[(R, R, V)]]
        .filter { case (r, c, v) => Ordering[R].equiv(r, c) }
        .map { _._3 }
      }.reduce(_ ++ _).sum.toPipe)
}

case class HadamardProduct[R, C, V](left: Matrix2[R, C, V],
  right: Matrix2[R, C, V],
  ring: Ring[V]) extends Matrix2[R, C, V] {

  // TODO: optimize / combine with Sums: https://github.com/tomtau/scalding/issues/14#issuecomment-22971582
  override lazy val toTypedPipe: TypedPipe[(R, C, V)] = {
    if (left.equals(right)) {
      left.optimizedSelf.toTypedPipe.map(v => (v._1, v._2, ring.times(v._3, v._3)))
    } else {
      val ord: Ordering[(R, C)] = Ordering.Tuple2(left.rowOrd, left.colOrd)
      // tracking values which were reduced (multiplied by non-zero) or non-reduced (multiplied by zero) with a boolean
      (left.optimizedSelf.toTypedPipe.map { case (r, c, v) => (r, c, (v, false)) } ++
        right.optimizedSelf.toTypedPipe.map { case (r, c, v) => (r, c, (v, false)) })
        .groupBy(x => (x._1, x._2))(ord)
        .mapValues { _._3 }
        .reduce((x, y) => (ring.times(x._1, y._1), true))
        .filter { kv => kv._2._2 && ring.isNonZero(kv._2._1) }
        .map { case ((r, c), v) => (r, c, v._1) }
    }
  }

  override lazy val transpose: MatrixLiteral[C, R, V] = MatrixLiteral(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)(colOrd, rowOrd)
  override val sizeHint = left.sizeHint #*# right.sizeHint
  override def negate(implicit g: Group[V]): HadamardProduct[R, C, V] =
    if (left.sizeHint.total.getOrElse(BigInt(0L)) > right.sizeHint.total.getOrElse(BigInt(0L)))
      HadamardProduct(left, right.negate, ring)
    else
      HadamardProduct(left.negate, right, ring)

  implicit override val rowOrd: Ordering[R] = left.rowOrd
  implicit override val colOrd: Ordering[C] = left.colOrd
}

case class MatrixLiteral[R, C, V](override val toTypedPipe: TypedPipe[(R, C, V)],
  override val sizeHint: SizeHint)(implicit override val rowOrd: Ordering[R], override val colOrd: Ordering[C])
  extends Matrix2[R, C, V] {

  override lazy val transpose: MatrixLiteral[C, R, V] =
    MatrixLiteral(toTypedPipe.map(x => (x._2, x._1, x._3)), sizeHint.transpose)(colOrd, rowOrd)

  override def negate(implicit g: Group[V]): MatrixLiteral[R, C, V] =
    MatrixLiteral(toTypedPipe.map(x => (x._1, x._2, g.negate(x._3))), sizeHint)
}

sealed trait Scalar2[V] extends Serializable {

  def +(that: Scalar2[V])(implicit mon: Monoid[V]): Scalar2[V]
  def -(that: Scalar2[V])(implicit g: Group[V]): Scalar2[V] = this + that.map(x => g.negate(x))
  def *(that: Scalar2[V])(implicit ring: Ring[V]): Scalar2[V]
  def /(that: Scalar2[V])(implicit f: Field[V]): Scalar2[V]
  def unary_-(implicit g: Group[V]): Scalar2[V] = this.map(x => g.negate(x))

  def divMatrix[R, C](that: Matrix2[R, C, V])(implicit f: Field[V]): MatrixLiteral[R, C, V]

  def *[R, C](that: Matrix2[R, C, V])(implicit ring: Ring[V]): Matrix2[R, C, V] =
    that match {
      case Product(left, right, _, expressions) => if (left.sizeHint.total.getOrElse(BigInt(0L)) > right.sizeHint.total.getOrElse(BigInt(0L))) Product(left, (this * right), ring, expressions) else Product(this * left, right, ring, expressions)
      case HadamardProduct(left, right, _) => if (left.sizeHint.total.getOrElse(BigInt(0L)) > right.sizeHint.total.getOrElse(BigInt(0L))) HadamardProduct(left, (this * right), ring) else HadamardProduct(this * left, right, ring)
      case s @ Sum(left, right, mon) => Sum(this * left, this * right, mon)
      case m @ MatrixLiteral(_, _) => timesLiteral(m) // handle literals her
      case x @ OneC() => Product(OneC[Unit, V](), toMatrix, ring).asInstanceOf[Matrix2[R, C, V]]
      case x @ OneR() => Product(toMatrix, OneR[Unit, V](), ring).asInstanceOf[Matrix2[R, C, V]]
    }

  def timesLiteral[R, C](that: MatrixLiteral[R, C, V])(implicit ring: Ring[V]): MatrixLiteral[R, C, V]
  def map[U](fn: V => U): Scalar2[U]
  def toMatrix: Matrix2[Unit, Unit, V]
  // TODO: FunctionMatrix[R,C,V](fn: (R,C) => V) and a Literal scalar is just: FuctionMatrix[Unit, Unit, V]({ (_, _) => v })
}

case class ScalarLiteral[V](v: V)(implicit @transient fd: FlowDef, @transient mode: Mode) extends Scalar2[V] {

  def +(that: Scalar2[V])(implicit mon: Monoid[V]): Scalar2[V] = that.map(x => mon.plus(v, x))
  def *(that: Scalar2[V])(implicit ring: Ring[V]): Scalar2[V] = that.map(x => ring.times(v, x))
  def /(that: Scalar2[V])(implicit f: Field[V]): Scalar2[V] = that.map(x => f.div(v, x))
  def divMatrix[R, C](that: Matrix2[R, C, V])(implicit f: Field[V]): MatrixLiteral[R, C, V] =
    MatrixLiteral(that.toTypedPipe.map(x => (x._1, x._2, f.div(x._3, v))), that.sizeHint)(that.rowOrd, that.colOrd)

  def timesLiteral[R, C](that: MatrixLiteral[R, C, V])(implicit ring: Ring[V]): MatrixLiteral[R, C, V] =
    MatrixLiteral(that.toTypedPipe.map(x => (x._1, x._2, ring.times(v, x._3))), that.sizeHint)(that.rowOrd, that.colOrd)

  def map[U](fn: V => U): Scalar2[U] = ScalarLiteral(fn(v))
  def toMatrix: Matrix2[Unit, Unit, V] =
    MatrixLiteral(TypedPipe.from(List(((),(),v))), FiniteHint(1,1))
}

case class ComputedScalar[V](v: TypedPipe[V]) extends Scalar2[V] {
  def +(that: Scalar2[V])(implicit mon: Monoid[V]): Scalar2[V] = {
    that match {
      case ScalarLiteral(v2) => this.map(mon.plus(_,v2))
      // TODO: optimize sums of scalars like sums of matrices: only one M/R pass for the whole Sum
      case ComputedScalar(v2) => ComputedScalar((v ++ v2).sum(mon))
    }
  }
  def *(that: Scalar2[V])(implicit ring: Ring[V]): Scalar2[V] = {
    that match {
      case ScalarLiteral(v2) => this.map(ring.times(_,v2))
      case ComputedScalar(v2) => ComputedScalar(v.cross(v2).map{case (x,y) => ring.times(x,y)})
    }
  }
  def /(that: Scalar2[V])(implicit f: Field[V]): Scalar2[V] =
    that match {
      case ScalarLiteral(v2) => this.map(f.div(_,v2))
      case ComputedScalar(v2) => ComputedScalar(v.cross(v2).map{case (x,y) => f.div(x,y)})
    }

  def divMatrix[R, C](that: Matrix2[R, C, V])(implicit f: Field[V]): MatrixLiteral[R, C, V] =
    MatrixLiteral(
      that.toTypedPipe
        .cross(v)
        .map { case (x, v) => (x._1, x._2, f.div(x._3, v)) },
      that.sizeHint
    )(that.rowOrd, that.colOrd)

  def timesLiteral[R, C](that: MatrixLiteral[R, C, V])(implicit ring: Ring[V]): MatrixLiteral[R, C, V] =
    MatrixLiteral(
      that.toTypedPipe
        .cross(v)
        .map { case (x, v) => (x._1, x._2, ring.times(v, x._3)) },
      that.sizeHint
    )(that.rowOrd, that.colOrd)

  def map[U](fn: V => U): Scalar2[U] = ComputedScalar(v.map(fn))
  def toMatrix: Matrix2[Unit, Unit, V] =
    MatrixLiteral(v.map(v => ((), (), v)), FiniteHint(1, 1))
}

object Scalar2 {
  implicit def apply[V](v: V)(implicit fd: FlowDef, m: Mode): Scalar2[V] = ScalarLiteral(v)
}

object Matrix2 {

  def apply[R:Ordering, C: Ordering, V](t: TypedPipe[(R, C, V)], hint: SizeHint): Matrix2[R, C, V] = MatrixLiteral(t, hint)
  def read[R, C, V](t: TypedSource[(R, C, V)],
    hint: SizeHint)(implicit ordr: Ordering[R],
      ordc: Ordering[C], fd: FlowDef, m: Mode): Matrix2[R, C, V] =
    MatrixLiteral(TypedPipe.from(t), hint)

  def J[R, C, V](implicit ordR: Ordering[R], ordC: Ordering[C], ring: Ring[V]) =
    Product(OneC[R, V]()(ordR), OneR[C, V]()(ordC), ring)

  /**
   * The original prototype that employs the standard O(n^3) dynamic programming
   * procedure to optimize a matrix chain factorization.
   *
   * Now, it also "prefers" more spread out / bushy / less deep factorization
   * which reflects more the Map/Reduce nature.
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
          // the original did not multiply by (k - i) and (j - k - 1) respectively (this achieves spread out trees)
          val cost = (k - i) * computeCosts(p, i, k) + (j - k - 1) * computeCosts(p, k + 1, j) +
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
        Product(left, right, ring.get, Some(sharedMap))
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
        case Product(left, right, ring, _) => {
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
