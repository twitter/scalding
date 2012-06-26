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

/**
 * Handles the implementation of various versions of MatrixProducts 
 */

//import com.twitter.scalding.mathematics.{Ring,Monoid,Group,Field}
import com.twitter.scalding.RichPipe
//We use some function/implicit conversions here
import com.twitter.scalding.Dsl._

import cascading.pipe.Pipe
import cascading.tuple.Fields

import scala.math.Ordering
import scala.annotation.tailrec

/** Allows us to sort matrices by approximate type
 */
object SizeHintOrdering extends Ordering[SizeHint] {
  def compare(left : SizeHint, right : SizeHint) : Int = {
    (left, right) match {
      case (NoClue, FiniteHint(_,_)) => 1
      case (FiniteHint(_,_),NoClue) => -1
      case (NoClue, NoClue) => 0
      // Both have a size:
      case _ => {
        if( left.total.isEmpty ) {
          1
        }
        else if (right.total.isEmpty) {
          -1
        }
        else {
          left.total.get.compareTo(right.total.get)
        }
      }
    }
  }
}

/** Abstracts the approach taken to join the two matrices
 */
abstract class MatrixJoiner {
  def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe
}
// TODO: Tune joinWithTiny such that it works
case object AnyToTiny extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    RichPipe(left).joinWithSmaller(joinFields, right)
  }
}
case object BigToSmall extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    RichPipe(left).joinWithSmaller(joinFields, right)
  }
}
// TODO: Tune joinWithTiny such that it works
case object TinyToAny extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    val reversed = (joinFields._2, joinFields._1)
    RichPipe(right).joinWithSmaller(reversed, left)
  }
}
case object SmallToBig extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    RichPipe(left).joinWithLarger(joinFields, right)
  }
}

trait MatrixProduct[Left,Right,Result] extends java.io.Serializable {
  def apply(left : Left, right : Right) : Result
}

/**
 * TODO: Muliplication is the expensive stuff.  We need to optimize the methods below:
 * This object holds the implicits to handle matrix products between various types
 */
object MatrixProduct {
  val MAX_TINY_JOIN = 100000L // Bigger than this, and we use joinWithSmaller
  def getJoiner(leftSize : SizeHint, rightSize : SizeHint) : MatrixJoiner = {
    if (SizeHintOrdering.lteq(leftSize, rightSize)) {
      // If leftsize is definite:
      leftSize.total.map { t => if (t < MAX_TINY_JOIN) TinyToAny else SmallToBig }
        // Else just assume the right is smaller, but both are unknown:
        .getOrElse(BigToSmall)
    }
    else {
      // left > right
      rightSize.total.map { rs =>
        if (rs < MAX_TINY_JOIN) AnyToTiny else BigToSmall
      }.getOrElse(BigToSmall)
    }
  }

  implicit def literalScalarRightProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[Row,Col,ValT],LiteralScalar[ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[Matrix[Row,Col,ValT],LiteralScalar[ValT],Matrix[Row,Col,ValT]] {
      def apply(left : Matrix[Row,Col,ValT], right : LiteralScalar[ValT]) = {
        val newPipe = left.pipe.map(left.valSym -> left.valSym) { (v : ValT) =>
          ring.times(v, right.value)
        }
        new Matrix[Row,Col,ValT](left.rowSym, left.colSym, left.valSym, newPipe, left.sizeHint)
      }
    }

  implicit def literalRightProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[Row,Col,ValT],ValT,Matrix[Row,Col,ValT]] =
    new MatrixProduct[Matrix[Row,Col,ValT],ValT,Matrix[Row,Col,ValT]] {
      def apply(left : Matrix[Row,Col,ValT], right : ValT) = {
        val newPipe = left.pipe.map(left.valSym -> left.valSym) { (v : ValT) =>
          ring.times(v, right)
        }
        new Matrix[Row,Col,ValT](left.rowSym, left.colSym, left.valSym, newPipe, left.sizeHint)
      }
    }


  implicit def literalScalarLeftProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[LiteralScalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[LiteralScalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] {
      def apply( left : LiteralScalar[ValT], right : Matrix[Row,Col,ValT]) = {
        val newPipe = right.pipe.map(right.valSym -> right.valSym) { (v : ValT) =>
          ring.times(left.value, v)
        }
        new Matrix[Row,Col,ValT](right.rowSym, right.colSym, right.valSym, newPipe, right.sizeHint)
      }
    }

  implicit def scalarPipeRightProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[Row,Col,ValT],Scalar[ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[Matrix[Row,Col,ValT],Scalar[ValT],Matrix[Row,Col,ValT]] {
      def apply(left : Matrix[Row,Col,ValT], right : Scalar[ValT]) = {
        left.nonZerosWith(right).mapValues({leftRight =>
          val (left, right) = leftRight
          ring.times(left, right)
        })(ring)
      }
    }

  implicit def scalarPipeLeftProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Scalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[Scalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] {
      def apply(left : Scalar[ValT], right : Matrix[Row,Col,ValT]) = {
        right.nonZerosWith(left).mapValues({matScal =>
          val (matVal, scalarVal) = matScal
          ring.times(scalarVal, matVal)
        })(ring)
      }
    }

  implicit def rowColProduct[IdxT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[RowVector[IdxT,ValT],ColVector[IdxT,ValT],Scalar[ValT]] =
    new MatrixProduct[RowVector[IdxT,ValT],ColVector[IdxT,ValT],Scalar[ValT]] {
      def apply(left : RowVector[IdxT,ValT], right : ColVector[IdxT,ValT]) : Scalar[ValT] = {
        // Normal matrix multiplication works here, but we need to convert to a Scalar
        val prod = (left.toMatrix(0) * right.toMatrix(0)) : Matrix[Int,Int,ValT]
        new Scalar[ValT](prod.valSym, prod.pipe.project(prod.valSym))
      }
    }

  implicit def standardMatrixProduct[RowL,Common,ColR,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[RowL,Common,ValT],Matrix[Common,ColR,ValT],Matrix[RowL,ColR,ValT]] =
    new MatrixProduct[Matrix[RowL,Common,ValT],Matrix[Common,ColR,ValT],Matrix[RowL,ColR,ValT]] {
      def apply(left : Matrix[RowL,Common,ValT], right : Matrix[Common,ColR,ValT]) = {
        val (newRightFields, newRightPipe) = Matrix.ensureUniqueFields(
          List(left.rowSym,left.colSym,left.valSym),
          List(right.rowSym, right.colSym, right.valSym),
          right.pipe
        )
        val newHint = left.sizeHint * right.sizeHint
        val productPipe = Matrix.filterOutZeros(left.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.colSym -> newRightFields(0)), newRightPipe)
            // Do the product:
            .map((left.valSym,newRightFields(2)) -> left.valSym) { pair : (ValT,ValT) =>
              ring.times(pair._1, pair._2)
            }
            .groupBy(left.rowSym, newRightFields(1)) {
              // We should use the size hints to set the number of reducers here
              _.reduce(left.valSym) { (x: Tuple1[ValT], y: Tuple1[ValT]) => Tuple1(ring.plus(x._1, y._1)) }
            }
          }
          // Keep the names from the left:
          .rename(newRightFields(1) -> left.colSym)
        new Matrix[RowL,ColR,ValT](left.rowSym, left.colSym, left.valSym, productPipe, newHint)
      }
  }

  // TODO: optimize this. We don't need to do the groupBy if the matrix is already diagonal
  implicit def diagMatrixProduct[RowT,ColT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[DiagonalMatrix[RowT,ValT],Matrix[RowT,ColT,ValT],Matrix[RowT,ColT,ValT]] =
    new MatrixProduct[DiagonalMatrix[RowT,ValT],Matrix[RowT,ColT,ValT],Matrix[RowT,ColT,ValT]] {
      def apply(left : DiagonalMatrix[RowT,ValT], right : Matrix[RowT,ColT,ValT]) = {
        Matrix.diagonalToMatrix(left) * right
      }
    }

  // TODO: optimize this. We don't need to do the groupBy if the matrix is already diagonal
  implicit def matrixDiagProduct[RowT,ColT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[RowT,ColT,ValT],DiagonalMatrix[ColT,ValT],Matrix[RowT,ColT,ValT]] =
    new MatrixProduct[Matrix[RowT,ColT,ValT],DiagonalMatrix[ColT,ValT],Matrix[RowT,ColT,ValT]] {
      def apply(left : Matrix[RowT,ColT,ValT], right : DiagonalMatrix[ColT,ValT]) = {
        left * Matrix.diagonalToMatrix(right)
      }
    }
}
