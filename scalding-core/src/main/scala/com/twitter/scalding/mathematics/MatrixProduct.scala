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

import com.twitter.algebird.Ring
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Dsl._

import cascading.pipe.Pipe
import cascading.tuple.Fields

/**
 * Abstracts the approach taken to join the two matrices
 */
abstract class MatrixJoiner extends java.io.Serializable {
  def apply(left: Pipe, joinFields: (Fields, Fields), right: Pipe): Pipe
}

case object AnyToTiny extends MatrixJoiner {
  override def apply(left: Pipe, joinFields: (Fields, Fields), right: Pipe): Pipe = {
    RichPipe(left).joinWithTiny(joinFields, right)
  }
}
class BigToSmall(red: Int) extends MatrixJoiner {
  override def apply(left: Pipe, joinFields: (Fields, Fields), right: Pipe): Pipe = {
    RichPipe(left).joinWithSmaller(joinFields, right, reducers = red)
  }
}

case object TinyToAny extends MatrixJoiner {
  override def apply(left: Pipe, joinFields: (Fields, Fields), right: Pipe): Pipe = {
    val reversed = (joinFields._2, joinFields._1)
    RichPipe(right).joinWithTiny(reversed, left)
  }
}
class SmallToBig(red: Int) extends MatrixJoiner {
  override def apply(left: Pipe, joinFields: (Fields, Fields), right: Pipe): Pipe = {
    RichPipe(left).joinWithLarger(joinFields, right, reducers = red)
  }
}

abstract class MatrixCrosser extends java.io.Serializable {
  def apply(left: Pipe, right: Pipe): Pipe
}

case object AnyCrossTiny extends MatrixCrosser {
  override def apply(left: Pipe, right: Pipe): Pipe = {
    RichPipe(left).crossWithTiny(right)
  }
}

case object AnyCrossSmall extends MatrixCrosser {
  override def apply(left: Pipe, right: Pipe): Pipe = {
    RichPipe(left).crossWithSmaller(right)
  }
}

trait MatrixProduct[Left, Right, Result] extends java.io.Serializable {
  def apply(left: Left, right: Right): Result
}

/**
 * TODO: Muliplication is the expensive stuff.  We need to optimize the methods below:
 * This object holds the implicits to handle matrix products between various types
 */
object MatrixProduct extends java.io.Serializable {
  // These are VARS, so you can set them before you start:
  var maxTinyJoin = 100000L // Bigger than this, and we use joinWithSmaller
  var maxReducers = 200

  def numOfReducers(hint: SizeHint) = {
    hint.total.map { tot =>
      // + 1L is to make sure there is at least once reducer
      (tot / MatrixProduct.maxTinyJoin + 1L).toInt min MatrixProduct.maxReducers
    }.getOrElse(-1)
  }

  def getJoiner(leftSize: SizeHint, rightSize: SizeHint): MatrixJoiner = {
    val newHint = leftSize * rightSize
    if (SizeHintOrdering.lteq(leftSize, rightSize)) {
      // If leftsize is definite:
      leftSize.total.map { t => if (t < maxTinyJoin) TinyToAny else new SmallToBig(numOfReducers(newHint)) }
        // Else just assume the right is smaller, but both are unknown:
        .getOrElse(new BigToSmall(numOfReducers(newHint)))
    } else {
      // left > right
      rightSize.total.map { rs =>
        if (rs < maxTinyJoin) AnyToTiny else new BigToSmall(numOfReducers(newHint))
      }.getOrElse(new BigToSmall(numOfReducers(newHint)))
    }
  }

  def getCrosser(rightSize: SizeHint): MatrixCrosser =
    rightSize.total.map { t => if (t < maxTinyJoin) AnyCrossTiny else AnyCrossSmall }
      .getOrElse(AnyCrossSmall)

  implicit def literalScalarRightProduct[Row, Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[Matrix[Row, Col, ValT], LiteralScalar[ValT], Matrix[Row, Col, ValT]] =
    new MatrixProduct[Matrix[Row, Col, ValT], LiteralScalar[ValT], Matrix[Row, Col, ValT]] {
      def apply(left: Matrix[Row, Col, ValT], right: LiteralScalar[ValT]) = {
        val newPipe = left.pipe.map(left.valSym -> left.valSym) { (v: ValT) =>
          ring.times(v, right.value)
        }
        new Matrix[Row, Col, ValT](left.rowSym, left.colSym, left.valSym, newPipe, left.sizeHint)
      }
    }

  implicit def literalRightProduct[Row, Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[Matrix[Row, Col, ValT], ValT, Matrix[Row, Col, ValT]] =
    new MatrixProduct[Matrix[Row, Col, ValT], ValT, Matrix[Row, Col, ValT]] {
      def apply(left: Matrix[Row, Col, ValT], right: ValT) = {
        val newPipe = left.pipe.map(left.valSym -> left.valSym) { (v: ValT) =>
          ring.times(v, right)
        }
        new Matrix[Row, Col, ValT](left.rowSym, left.colSym, left.valSym, newPipe, left.sizeHint)
      }
    }

  implicit def literalScalarLeftProduct[Row, Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[LiteralScalar[ValT], Matrix[Row, Col, ValT], Matrix[Row, Col, ValT]] =
    new MatrixProduct[LiteralScalar[ValT], Matrix[Row, Col, ValT], Matrix[Row, Col, ValT]] {
      def apply(left: LiteralScalar[ValT], right: Matrix[Row, Col, ValT]) = {
        val newPipe = right.pipe.map(right.valSym -> right.valSym) { (v: ValT) =>
          ring.times(left.value, v)
        }
        new Matrix[Row, Col, ValT](right.rowSym, right.colSym, right.valSym, newPipe, right.sizeHint)
      }
    }

  implicit def scalarPipeRightProduct[Row, Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[Matrix[Row, Col, ValT], Scalar[ValT], Matrix[Row, Col, ValT]] =
    new MatrixProduct[Matrix[Row, Col, ValT], Scalar[ValT], Matrix[Row, Col, ValT]] {
      def apply(left: Matrix[Row, Col, ValT], right: Scalar[ValT]) = {
        left.nonZerosWith(right).mapValues({ leftRight =>
          val (left, right) = leftRight
          ring.times(left, right)
        })(ring)
      }
    }

  implicit def scalarPipeLeftProduct[Row, Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[Scalar[ValT], Matrix[Row, Col, ValT], Matrix[Row, Col, ValT]] =
    new MatrixProduct[Scalar[ValT], Matrix[Row, Col, ValT], Matrix[Row, Col, ValT]] {
      def apply(left: Scalar[ValT], right: Matrix[Row, Col, ValT]) = {
        right.nonZerosWith(left).mapValues({ matScal =>
          val (matVal, scalarVal) = matScal
          ring.times(scalarVal, matVal)
        })(ring)
      }
    }

  implicit def scalarRowRightProduct[Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[RowVector[Col, ValT], Scalar[ValT], RowVector[Col, ValT]] =
    new MatrixProduct[RowVector[Col, ValT], Scalar[ValT], RowVector[Col, ValT]] {
      def apply(left: RowVector[Col, ValT], right: Scalar[ValT]): RowVector[Col, ValT] = {
        val prod = left.toMatrix(0) * right

        new RowVector[Col, ValT](prod.colSym, prod.valSym, prod.pipe.project(prod.colSym, prod.valSym))
      }
    }

  implicit def scalarRowLeftProduct[Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[Scalar[ValT], RowVector[Col, ValT], RowVector[Col, ValT]] =
    new MatrixProduct[Scalar[ValT], RowVector[Col, ValT], RowVector[Col, ValT]] {
      def apply(left: Scalar[ValT], right: RowVector[Col, ValT]): RowVector[Col, ValT] = {
        val prod = (right.transpose.toMatrix(0)) * left

        new RowVector[Col, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def scalarColRightProduct[Row, ValT](implicit ring: Ring[ValT]): MatrixProduct[ColVector[Row, ValT], Scalar[ValT], ColVector[Row, ValT]] =
    new MatrixProduct[ColVector[Row, ValT], Scalar[ValT], ColVector[Row, ValT]] {
      def apply(left: ColVector[Row, ValT], right: Scalar[ValT]): ColVector[Row, ValT] = {
        val prod = left.toMatrix(0) * right

        new ColVector[Row, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def scalarColLeftProduct[Row, ValT](implicit ring: Ring[ValT]): MatrixProduct[Scalar[ValT], ColVector[Row, ValT], ColVector[Row, ValT]] =
    new MatrixProduct[Scalar[ValT], ColVector[Row, ValT], ColVector[Row, ValT]] {
      def apply(left: Scalar[ValT], right: ColVector[Row, ValT]): ColVector[Row, ValT] = {
        val prod = (right.toMatrix(0)) * left

        new ColVector[Row, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def litScalarRowRightProduct[Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[RowVector[Col, ValT], LiteralScalar[ValT], RowVector[Col, ValT]] =
    new MatrixProduct[RowVector[Col, ValT], LiteralScalar[ValT], RowVector[Col, ValT]] {
      def apply(left: RowVector[Col, ValT], right: LiteralScalar[ValT]): RowVector[Col, ValT] = {
        val prod = left.toMatrix(0) * right

        new RowVector[Col, ValT](prod.colSym, prod.valSym, prod.pipe.project(prod.colSym, prod.valSym))
      }
    }

  implicit def litScalarRowLeftProduct[Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[LiteralScalar[ValT], RowVector[Col, ValT], RowVector[Col, ValT]] =
    new MatrixProduct[LiteralScalar[ValT], RowVector[Col, ValT], RowVector[Col, ValT]] {
      def apply(left: LiteralScalar[ValT], right: RowVector[Col, ValT]): RowVector[Col, ValT] = {
        val prod = (right.transpose.toMatrix(0)) * left

        new RowVector[Col, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def litScalarColRightProduct[Row, ValT](implicit ring: Ring[ValT]): MatrixProduct[ColVector[Row, ValT], LiteralScalar[ValT], ColVector[Row, ValT]] =
    new MatrixProduct[ColVector[Row, ValT], LiteralScalar[ValT], ColVector[Row, ValT]] {
      def apply(left: ColVector[Row, ValT], right: LiteralScalar[ValT]): ColVector[Row, ValT] = {
        val prod = left.toMatrix(0) * right

        new ColVector[Row, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def litScalarColLeftProduct[Row, ValT](implicit ring: Ring[ValT]): MatrixProduct[LiteralScalar[ValT], ColVector[Row, ValT], ColVector[Row, ValT]] =
    new MatrixProduct[LiteralScalar[ValT], ColVector[Row, ValT], ColVector[Row, ValT]] {
      def apply(left: LiteralScalar[ValT], right: ColVector[Row, ValT]): ColVector[Row, ValT] = {
        val prod = (right.toMatrix(0)) * left

        new ColVector[Row, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def scalarDiagRightProduct[Row, ValT](implicit ring: Ring[ValT]): MatrixProduct[DiagonalMatrix[Row, ValT], Scalar[ValT], DiagonalMatrix[Row, ValT]] =
    new MatrixProduct[DiagonalMatrix[Row, ValT], Scalar[ValT], DiagonalMatrix[Row, ValT]] {
      def apply(left: DiagonalMatrix[Row, ValT], right: Scalar[ValT]): DiagonalMatrix[Row, ValT] = {
        val prod = (left.toCol.toMatrix(0)) * right

        new DiagonalMatrix[Row, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def scalarDiagLeftProduct[Row, ValT](implicit ring: Ring[ValT]): MatrixProduct[Scalar[ValT], DiagonalMatrix[Row, ValT], DiagonalMatrix[Row, ValT]] =
    new MatrixProduct[Scalar[ValT], DiagonalMatrix[Row, ValT], DiagonalMatrix[Row, ValT]] {
      def apply(left: Scalar[ValT], right: DiagonalMatrix[Row, ValT]): DiagonalMatrix[Row, ValT] = {
        val prod = (right.toCol.toMatrix(0)) * left

        new DiagonalMatrix[Row, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  implicit def litScalarDiagRightProduct[Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[DiagonalMatrix[Col, ValT], LiteralScalar[ValT], DiagonalMatrix[Col, ValT]] =
    new MatrixProduct[DiagonalMatrix[Col, ValT], LiteralScalar[ValT], DiagonalMatrix[Col, ValT]] {
      def apply(left: DiagonalMatrix[Col, ValT], right: LiteralScalar[ValT]): DiagonalMatrix[Col, ValT] = {
        val prod = (left.toRow.toMatrix(0)) * right

        new DiagonalMatrix[Col, ValT](prod.colSym, prod.valSym, prod.pipe.project(prod.colSym, prod.valSym))
      }
    }

  implicit def litScalarDiagLeftProduct[Col, ValT](implicit ring: Ring[ValT]): MatrixProduct[LiteralScalar[ValT], DiagonalMatrix[Col, ValT], DiagonalMatrix[Col, ValT]] =
    new MatrixProduct[LiteralScalar[ValT], DiagonalMatrix[Col, ValT], DiagonalMatrix[Col, ValT]] {
      def apply(left: LiteralScalar[ValT], right: DiagonalMatrix[Col, ValT]): DiagonalMatrix[Col, ValT] = {
        val prod = (right.toCol.toMatrix(0)) * left

        new DiagonalMatrix[Col, ValT](prod.rowSym, prod.valSym, prod.pipe.project(prod.rowSym, prod.valSym))
      }
    }

  //TODO: remove in 0.9.0, only here just for compatibility.
  def vectorInnerProduct[IdxT, ValT](implicit ring: Ring[ValT]): MatrixProduct[RowVector[IdxT, ValT], ColVector[IdxT, ValT], Scalar[ValT]] =
    rowColProduct(ring)

  implicit def rowColProduct[IdxT, ValT](implicit ring: Ring[ValT]): MatrixProduct[RowVector[IdxT, ValT], ColVector[IdxT, ValT], Scalar[ValT]] =
    new MatrixProduct[RowVector[IdxT, ValT], ColVector[IdxT, ValT], Scalar[ValT]] {
      def apply(left: RowVector[IdxT, ValT], right: ColVector[IdxT, ValT]): Scalar[ValT] = {
        // Normal matrix multiplication works here, but we need to convert to a Scalar
        val prod = (left.toMatrix(0) * right.toMatrix(0)): Matrix[Int, Int, ValT]
        new Scalar[ValT](prod.valSym, prod.pipe.project(prod.valSym))
      }
    }

  implicit def rowMatrixProduct[Common, ColR, ValT](implicit ring: Ring[ValT]): MatrixProduct[RowVector[Common, ValT], Matrix[Common, ColR, ValT], RowVector[ColR, ValT]] =
    new MatrixProduct[RowVector[Common, ValT], Matrix[Common, ColR, ValT], RowVector[ColR, ValT]] {
      def apply(left: RowVector[Common, ValT], right: Matrix[Common, ColR, ValT]) = {
        (left.toMatrix(true) * right).getRow(true)
      }
    }

  implicit def matrixColProduct[RowR, Common, ValT](implicit ring: Ring[ValT]): MatrixProduct[Matrix[RowR, Common, ValT], ColVector[Common, ValT], ColVector[RowR, ValT]] =
    new MatrixProduct[Matrix[RowR, Common, ValT], ColVector[Common, ValT], ColVector[RowR, ValT]] {
      def apply(left: Matrix[RowR, Common, ValT], right: ColVector[Common, ValT]) = {
        (left * right.toMatrix(true)).getCol(true)
      }
    }

  implicit def vectorOuterProduct[RowT, ColT, ValT](implicit ring: Ring[ValT]): MatrixProduct[ColVector[RowT, ValT], RowVector[ColT, ValT], Matrix[RowT, ColT, ValT]] =
    new MatrixProduct[ColVector[RowT, ValT], RowVector[ColT, ValT], Matrix[RowT, ColT, ValT]] {
      def apply(left: ColVector[RowT, ValT], right: RowVector[ColT, ValT]): Matrix[RowT, ColT, ValT] = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.rowS, left.valS),
          (right.colS, right.valS),
          right.pipe)
        val newColSym = Symbol(right.colS.name + "_newCol")
        val newHint = left.sizeH * right.sizeH
        val productPipe = Matrix.filterOutZeros(left.valS, ring) {
          getCrosser(right.sizeH)
            .apply(left.pipe, newRightPipe)
            .map(left.valS.append(getField(newRightFields, 1)) -> left.valS) { pair: (ValT, ValT) =>
              ring.times(pair._1, pair._2)
            }
        }
          .rename(getField(newRightFields, 0) -> newColSym)
        new Matrix[RowT, ColT, ValT](left.rowS, newColSym, left.valS, productPipe, newHint)
      }
    }

  implicit def standardMatrixProduct[RowL, Common, ColR, ValT](implicit ring: Ring[ValT]): MatrixProduct[Matrix[RowL, Common, ValT], Matrix[Common, ColR, ValT], Matrix[RowL, ColR, ValT]] =
    new MatrixProduct[Matrix[RowL, Common, ValT], Matrix[Common, ColR, ValT], Matrix[RowL, ColR, ValT]] {
      def apply(left: Matrix[RowL, Common, ValT], right: Matrix[Common, ColR, ValT]) = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.rowSym, left.colSym, left.valSym),
          (right.rowSym, right.colSym, right.valSym),
          right.pipe)
        val newHint = left.sizeHint * right.sizeHint
        // Hint of groupBy reducer size
        val grpReds = numOfReducers(newHint)

        val productPipe = Matrix.filterOutZeros(left.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.colSym -> getField(newRightFields, 0)), newRightPipe)
            // Do the product:
            .map((left.valSym.append(getField(newRightFields, 2))) -> left.valSym) { pair: (ValT, ValT) =>
              ring.times(pair._1, pair._2)
            }
            .groupBy(left.rowSym.append(getField(newRightFields, 1))) {
              // We should use the size hints to set the number of reducers here
              _.reduce(left.valSym) { (x: Tuple1[ValT], y: Tuple1[ValT]) => Tuple1(ring.plus(x._1, y._1)) }
                // There is a low chance that many (row,col) keys are co-located, and the keyspace
                // is likely huge, just push to reducers
                .forceToReducers
                .reducers(grpReds)
            }
        }
          // Keep the names from the left:
          .rename(getField(newRightFields, 1) -> left.colSym)
        new Matrix[RowL, ColR, ValT](left.rowSym, left.colSym, left.valSym, productPipe, newHint)
      }
    }

  implicit def diagMatrixProduct[RowT, ColT, ValT](implicit ring: Ring[ValT]): MatrixProduct[DiagonalMatrix[RowT, ValT], Matrix[RowT, ColT, ValT], Matrix[RowT, ColT, ValT]] =
    new MatrixProduct[DiagonalMatrix[RowT, ValT], Matrix[RowT, ColT, ValT], Matrix[RowT, ColT, ValT]] {
      def apply(left: DiagonalMatrix[RowT, ValT], right: Matrix[RowT, ColT, ValT]) = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.idxSym, left.valSym),
          (right.rowSym, right.colSym, right.valSym),
          right.pipe)
        val newHint = left.sizeHint * right.sizeHint
        val productPipe = Matrix.filterOutZeros(right.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.idxSym -> getField(newRightFields, 0)), newRightPipe)
            // Do the product:
            .map((left.valSym.append(getField(newRightFields, 2))) -> getField(newRightFields, 2)) { pair: (ValT, ValT) =>
              ring.times(pair._1, pair._2)
            }
            // Keep the names from the right:
            .project(newRightFields)
            .rename(newRightFields -> (right.rowSym, right.colSym, right.valSym))
        }
        new Matrix[RowT, ColT, ValT](right.rowSym, right.colSym, right.valSym, productPipe, newHint)
      }
    }

  implicit def matrixDiagProduct[RowT, ColT, ValT](implicit ring: Ring[ValT]): MatrixProduct[Matrix[RowT, ColT, ValT], DiagonalMatrix[ColT, ValT], Matrix[RowT, ColT, ValT]] =
    new MatrixProduct[Matrix[RowT, ColT, ValT], DiagonalMatrix[ColT, ValT], Matrix[RowT, ColT, ValT]] {
      def apply(left: Matrix[RowT, ColT, ValT], right: DiagonalMatrix[ColT, ValT]) = {
        // (A * B) = (B^T * A^T)^T
        // note diagonal^T = diagonal
        (right * (left.transpose)).transpose
      }
    }

  implicit def diagDiagProduct[IdxT, ValT](implicit ring: Ring[ValT]): MatrixProduct[DiagonalMatrix[IdxT, ValT], DiagonalMatrix[IdxT, ValT], DiagonalMatrix[IdxT, ValT]] =
    new MatrixProduct[DiagonalMatrix[IdxT, ValT], DiagonalMatrix[IdxT, ValT], DiagonalMatrix[IdxT, ValT]] {
      def apply(left: DiagonalMatrix[IdxT, ValT], right: DiagonalMatrix[IdxT, ValT]) = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.idxSym, left.valSym),
          (right.idxSym, right.valSym),
          right.pipe)
        val newHint = left.sizeHint * right.sizeHint
        val productPipe = Matrix.filterOutZeros(left.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.idxSym -> getField(newRightFields, 0)), newRightPipe)
            // Do the product:
            .map((left.valSym.append(getField(newRightFields, 1))) -> left.valSym) { pair: (ValT, ValT) =>
              ring.times(pair._1, pair._2)
            }
        }
          // Keep the names from the left:
          .project(left.idxSym, left.valSym)
        new DiagonalMatrix[IdxT, ValT](left.idxSym, left.valSym, productPipe, newHint)
      }
    }

  implicit def diagColProduct[IdxT, ValT](implicit ring: Ring[ValT]): MatrixProduct[DiagonalMatrix[IdxT, ValT], ColVector[IdxT, ValT], ColVector[IdxT, ValT]] =
    new MatrixProduct[DiagonalMatrix[IdxT, ValT], ColVector[IdxT, ValT], ColVector[IdxT, ValT]] {
      def apply(left: DiagonalMatrix[IdxT, ValT], right: ColVector[IdxT, ValT]) = {
        (left * (right.diag)).toCol
      }
    }
  implicit def rowDiagProduct[IdxT, ValT](implicit ring: Ring[ValT]): MatrixProduct[RowVector[IdxT, ValT], DiagonalMatrix[IdxT, ValT], RowVector[IdxT, ValT]] =
    new MatrixProduct[RowVector[IdxT, ValT], DiagonalMatrix[IdxT, ValT], RowVector[IdxT, ValT]] {
      def apply(left: RowVector[IdxT, ValT], right: DiagonalMatrix[IdxT, ValT]) = {
        ((left.diag) * right).toRow
      }
    }
}
