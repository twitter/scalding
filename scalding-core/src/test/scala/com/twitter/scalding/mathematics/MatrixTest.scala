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

import com.twitter.scalding._
import cascading.pipe.joiner._
import org.scalatest.{ Matchers, WordSpec }
import com.twitter.algebird.Group
import com.twitter.algebird.field._

object TUtil {
  def printStack(fn: => Unit): Unit = {
    try { fn } catch { case e: Throwable => e.printStackTrace; throw e }
  }
}

class MatrixProd(args: Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1", ('x1, 'y1, 'v1))
    .toMatrix[Int, Int, Double]('x1, 'y1, 'v1)

  val gram = mat1 * mat1.transpose
  gram.pipe.write(Tsv("product"))
}

class MatrixBlockProd(args: Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1", ('x1, 'y1, 'v1))
    .mapToBlockMatrix(('x1, 'y1, 'v1)) { (rcv: (String, Int, Double)) => (rcv._1(0), rcv._1, rcv._2, rcv._3) }

  val mat2 = Tsv("mat1", ('x1, 'y1, 'v1))
    .toMatrix[String, Int, Double]('x1, 'y1, 'v1)
    .toBlockMatrix(s => (s(0), s))

  val gram = mat1 dotProd mat2.transpose
  gram.pipe.write(Tsv("product"))
}

class MatrixSum(args: Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1", ('x1, 'y1, 'v1))
    .mapToMatrix('x1, 'y1, 'v1) { rowColVal: (Int, Int, Double) => rowColVal }
  val mat2 = Tsv("mat2", ('x2, 'y2, 'v2))
    .mapToMatrix('x2, 'y2, 'v2) { rowColVal: (Int, Int, Double) => rowColVal }

  val sum = mat1 + mat2
  sum.pipe.write(Tsv("sum"))
}

class MatrixSum3(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, (Double, Double, Double)]('x1, 'y1, 'v1, p1)

  val sum = mat1 + mat1
  sum.pipe.write(Tsv("sum"))
}

class Randwalk(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val mat1L1Norm = mat1.rowL1Normalize
  val randwalk = mat1L1Norm * mat1L1Norm
  randwalk.pipe.write(Tsv("randwalk"))
}

class Cosine(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val matL2Norm = mat1.rowL2Normalize
  val cosine = matL2Norm * matL2Norm.transpose
  cosine.pipe.write(Tsv("cosine"))
}

class Covariance(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val matCentered = mat1.colMeanCentering
  val cov = matCentered * matCentered.transpose
  cov.pipe.write(Tsv("cov"))
}

class VctProd(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row = mat1.getRow(1)
  val rowProd = row * row.transpose
  rowProd.pipe.write(Tsv("vctProd"))
}

class VctDiv(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row = mat1.getRow(1).diag
  val row2 = mat1.getRow(2).diag.inverse
  val rowDiv = row * row2
  rowDiv.pipe.write(Tsv("vctDiv"))
}

class ScalarOps(args: Args) extends Job(args) {
  import Matrix._
  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)
  (mat1 * 3.0).pipe.write(Tsv("times3"))
  (mat1 / 3.0).pipe.write(Tsv("div3"))
  (3.0 * mat1).pipe.write(Tsv("3times"))
  // Now with Scalar objects:
  (mat1.trace * mat1).pipe.write(Tsv("tracetimes"))
  (mat1 * mat1.trace).pipe.write(Tsv("timestrace"))
  (mat1 / mat1.trace).pipe.write(Tsv("divtrace"))
}

class DiagonalOps(args: Args) extends Job(args) {
  import Matrix._
  val mat = Tsv("mat1", ('x1, 'y1, 'v1))
    .read
    .toMatrix[Int, Int, Double]('x1, 'y1, 'v1)
  (mat * mat.diagonal).write(Tsv("mat-diag"))
  (mat.diagonal * mat).write(Tsv("diag-mat"))
  (mat.diagonal * mat.diagonal).write(Tsv("diag-diag"))
  (mat.diagonal * mat.getCol(1)).write(Tsv("diag-col"))
  (mat.getRow(1) * mat.diagonal).write(Tsv("row-diag"))
}

class PropJob(args: Args) extends Job(args) {
  import Matrix._

  val mat = TypedTsv[(Int, Int, Int)]("graph").toMatrix
  val row = TypedTsv[(Int, Double)]("row").toRow
  val col = TypedTsv[(Int, Double)]("col").toCol

  mat.binarizeAs[Boolean].propagate(col).write(Tsv("prop-col"))
  row.propagate(mat.binarizeAs[Boolean]).write(Tsv("prop-row"))
}

class MatrixMapWithVal(args: Args) extends Job(args) {
  import Matrix._

  val mat = TypedTsv[(Int, Int, Int)]("graph").toMatrix
  val row = TypedTsv[(Int, Double)]("row").toRow

  mat.mapWithIndex { (v, r, c) => if (r == c) v else 0 }.write(Tsv("diag"))
  row.mapWithIndex { (v, c) => if (c == 0) v else 0.0 }.write(Tsv("first"))
}

class RowMatProd(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row = mat1.getRow(1)
  val rowProd = row * mat1
  rowProd.pipe.write(Tsv("rowMatPrd"))
}

class MatColProd(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val col = mat1.getCol(1)
  val colProd = mat1 * col
  colProd.pipe.write(Tsv("matColPrd"))
}

class RowRowSum(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row1 = mat1.getRow(1)
  val rowSum = row1 + row1
  rowSum.pipe.write(Tsv("rowRowSum"))
}

class RowRowDiff(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row1 = mat1.getRow(1)
  val row2 = mat1.getRow(2)
  val rowSum = row1 - row2
  rowSum.pipe.write(Tsv("rowRowDiff"))
}

class RowRowHad(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row1 = mat1.getRow(1)
  val rowSum = row1 hProd row1
  rowSum.pipe.write(Tsv("rowRowHad"))
}

class VctOuterProd(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val mat1 = new Matrix[Int, Int, Double]('x1, 'y1, 'v1, p1)

  val row1 = mat1.getRow(1)
  val outerProd = row1.transpose * row1
  outerProd.pipe.write(Tsv("outerProd"))
}

class FilterMatrix(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x, 'y, 'v)).read
  val p2 = Tsv("mat2", ('x, 'y, 'v)).read
  val mat1 = new Matrix[Int, Int, Double]('x, 'y, 'v, p1)
  val mat2 = new Matrix[Int, Int, Double]('x, 'y, 'v, p2)

  mat1.removeElementsBy(mat2).write(Tsv("removeMatrix"))
  mat1.keepElementsBy(mat2).write(Tsv("keepMatrix"))
}

class KeepRowsCols(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x, 'y, 'v)).read
  val mat1 = new Matrix[Int, Int, Double]('x, 'y, 'v, p1)
  val p2 = Tsv("col1", ('x, 'v)).read
  val col1 = new ColVector[Int, Double]('x, 'v, p2)

  mat1.keepRowsBy(col1).write(Tsv("keepRows"))
  mat1.keepColsBy(col1.transpose).write(Tsv("keepCols"))
}

class RemoveRowsCols(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1", ('x, 'y, 'v)).read
  val mat1 = new Matrix[Int, Int, Double]('x, 'y, 'v, p1)
  val p2 = Tsv("col1", ('x, 'v)).read
  val col1 = new ColVector[Int, Double]('x, 'v, p2)

  mat1.removeRowsBy(col1).write(Tsv("removeRows"))
  mat1.removeColsBy(col1.transpose).write(Tsv("removeCols"))
}

class ScalarRowRight(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("row1", ('x, 'v)).read
  val row1 = new RowVector[Int, Double]('x, 'v, p1)

  (row1 * new LiteralScalar[Double](3.0)).write(Tsv("scalarRowRight"))

  // now with a scalar object

  val p2 = Tsv("sca1", ('v)).read
  val sca1 = new Scalar[Double]('v, p2)

  (row1 * sca1).write(Tsv("scalarObjRowRight"))
}

class ScalarRowLeft(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("row1", ('x, 'v)).read
  val row1 = new RowVector[Int, Double]('x, 'v, p1)

  (new LiteralScalar[Double](3.0) * row1).write(Tsv("scalarRowLeft"))

  // now with a scalar object

  val p2 = Tsv("sca1", ('v)).read
  val sca1 = new Scalar[Double]('v, p2)

  (sca1 * row1).write(Tsv("scalarObjRowLeft"))
}

class ScalarColRight(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("col1", ('x, 'v)).read
  val col1 = new ColVector[Int, Double]('x, 'v, p1)

  (col1 * new LiteralScalar[Double](3.0)).write(Tsv("scalarColRight"))

  // now with a scalar object

  val p2 = Tsv("sca1", ('v)).read
  val sca1 = new Scalar[Double]('v, p2)

  (col1 * sca1).write(Tsv("scalarObjColRight"))
}

class ScalarColLeft(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("col1", ('x, 'v)).read
  val col1 = new ColVector[Int, Double]('x, 'v, p1)

  (new LiteralScalar[Double](3.0) * col1).write(Tsv("scalarColLeft"))

  // now with a scalar object

  val p2 = Tsv("sca1", ('v)).read
  val sca1 = new Scalar[Double]('v, p2)

  (sca1 * col1).write(Tsv("scalarObjColLeft"))
}

class ScalarDiagRight(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("diag1", ('x, 'v)).read
  val diag1 = new DiagonalMatrix[Int, Double]('x, 'v, p1)

  (diag1 * new LiteralScalar[Double](3.0)).write(Tsv("scalarDiagRight"))

  // now with a scalar object

  val p2 = Tsv("sca1", ('v)).read
  val sca1 = new Scalar[Double]('v, p2)

  (diag1 * sca1).write(Tsv("scalarObjDiagRight"))
}

class ScalarDiagLeft(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("diag1", ('x, 'v)).read
  val diag1 = new DiagonalMatrix[Int, Double]('x, 'v, p1)

  (new LiteralScalar[Double](3.0) * diag1).write(Tsv("scalarDiagLeft"))

  // now with a scalar object

  val p2 = Tsv("sca1", ('v)).read
  val sca1 = new Scalar[Double]('v, p2)

  (sca1 * diag1).write(Tsv("scalarObjDiagLeft"))
}

class ColNormalize(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("col1", ('x, 'v)).read
  val col1 = new ColVector[Int, Double]('x, 'v, p1)

  col1.L0Normalize.write(Tsv("colLZeroNorm"))
  col1.L1Normalize.write(Tsv("colLOneNorm"))
}

class ColDiagonal(args: Args) extends Job(args) {

  import Matrix._

  val col1 = new ColVector[Int, Double]('x, 'v, null, FiniteHint(100, 1))

  val sizeHintTotal = col1.diag.sizeHint.total.get
}

class RowNormalize(args: Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("row1", ('x, 'v)).read
  val row1 = new RowVector[Int, Double]('x, 'v, p1)

  row1.L0Normalize.write(Tsv("rowLZeroNorm"))
  row1.L1Normalize.write(Tsv("rowLOneNorm"))
}

class MatrixTest extends WordSpec with Matchers {
  import Dsl._

  def toSparseMat[Row, Col, V](iter: Iterable[(Row, Col, V)]): Map[(Row, Col), V] = {
    iter.map { it => ((it._1, it._2), it._3) }.toMap
  }
  def oneDtoSparseMat[Idx, V](iter: Iterable[(Idx, V)]): Map[(Idx, Idx), V] = {
    iter.map { it => ((it._1, it._1), it._2) }.toMap
  }

  "A MatrixProd job" should {
    TUtil.printStack {
      JobTest(new MatrixProd(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("product")) { ob =>
          "correctly compute products" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 17.0, (1, 2) -> 12.0, (2, 1) -> 12.0, (2, 2) -> 9.0)
          }
        }
        .run
        .finish()
    }
  }

  "A MatrixBlockProd job" should {
    TUtil.printStack {
      JobTest(new MatrixBlockProd(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List(("alpha1", 1, 1.0), ("alpha1", 2, 2.0), ("beta1", 1, 5.0), ("beta1", 2, 6.0), ("alpha2", 1, 3.0), ("alpha2", 2, 4.0), ("beta2", 1, 7.0), ("beta2", 2, 8.0)))
        .sink[(String, String, Double)](Tsv("product")) { ob =>
          "correctly compute block products" in {
            toSparseMat(ob) shouldBe Map(
              ("alpha1", "alpha1") -> 5.0,
              ("alpha1", "alpha2") -> 11.0,
              ("alpha2", "alpha1") -> 11.0,
              ("alpha2", "alpha2") -> 25.0,
              ("beta1", "beta1") -> 61.0,
              ("beta1", "beta2") -> 83.0,
              ("beta2", "beta1") -> 83.0,
              ("beta2", "beta2") -> 113.0)
          }
        }
        .run
        .finish()
    }
  }

  "A MatrixSum job" should {
    TUtil.printStack {
      JobTest(new MatrixSum(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .source(Tsv("mat2", ('x2, 'y2, 'v2)), List((1, 3, 3.0), (2, 1, 8.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("sum")) { ob =>
          "correctly compute sums" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (1, 2) -> 8.0, (1, 3) -> 3.0, (2, 1) -> 8.0, (2, 2) -> 3.0)
          }
        }
        .run
        .finish()
    }
  }

  "A MatrixSum job, where the Matrix contains tuples as values," should {
    TUtil.printStack {
      JobTest("com.twitter.scalding.mathematics.MatrixSum3")
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, (1.0, 3.0, 5.0)), (2, 2, (3.0, 2.0, 1.0)), (1, 2, (4.0, 5.0, 2.0))))
        .sink[(Int, Int, (Double, Double, Double))](Tsv("sum")) { ob =>
          "correctly compute sums" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> (2.0, 6.0, 10.0), (2, 2) -> (6.0, 4.0, 2.0), (1, 2) -> (8.0, 10.0, 4.0))
          }
        }
        .run
        .finish()
    }
  }

  "A Matrix Randwalk job" should {
    TUtil.printStack {
      JobTest(new Randwalk(_))
        /*
       * 1.0 4.0
       * 0.0 3.0
       * row normalized:
       * 1.0/5.0 4.0/5.0
       * 0.0 1.0
       * product with itself:
       * 1.0/25.0 (4.0/25.0 + 4.0/5.0)
       * 0.0 1.0
       */
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("randwalk")) { ob =>
          "correctly compute matrix randwalk" in {
            val pMap = toSparseMat(ob)
            val exact = Map((1, 1) -> (1.0 / 25.0), (1, 2) -> (4.0 / 25.0 + 4.0 / 5.0), (2, 2) -> 1.0)
            val grp = implicitly[Group[Map[(Int, Int), Double]]]
            // doubles are hard to compare
            grp.minus(pMap, exact)
              .mapValues { x => x * x }
              .map { _._2 }
              .sum should be < 0.0001
          }
        }
        .run
        .finish()
    }
  }
  "A Matrix Cosine job" should {
    TUtil.printStack {
      JobTest(new Cosine(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("cosine")) { ob =>
          "correctly compute cosine similarity" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (1, 2) -> 0.9701425001453319, (2, 1) -> 0.9701425001453319, (2, 2) -> 1.0)
          }
        }
        .run
        .finish()
    }
  }
  "A Matrix Covariance job" should {
    TUtil.printStack {
      JobTest(new Covariance(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("cov")) { ob =>
          "correctly compute matrix covariance" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 0.25, (1, 2) -> -0.25, (2, 1) -> -0.25, (2, 2) -> 0.25)
          }
        }
        .run
        .finish()
    }
  }
  "A Matrix VctProd job" should {
    TUtil.printStack {
      JobTest(new VctProd(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[Double](Tsv("vctProd")) { ob =>
          "correctly compute vector inner products" in {
            ob(0) shouldBe 17.0
          }
        }
        .run
        .finish()
    }
  }
  "A Matrix VctDiv job" should {
    TUtil.printStack {
      JobTest(new VctDiv(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Double)](Tsv("vctDiv")) { ob =>
          "correctly compute vector element-wise division" in {
            oneDtoSparseMat(ob) shouldBe Map((2, 2) -> 1.3333333333333333)
          }
        }
        .run
        .finish()
    }
  }
  "A Matrix ScalarOps job" should {
    TUtil.printStack {
      JobTest(new ScalarOps(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("times3")) { ob =>
          "correctly compute M * 3" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 3.0, (2, 2) -> 9.0, (1, 2) -> 12.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("3times")) { ob =>
          "correctly compute 3 * M" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 3.0, (2, 2) -> 9.0, (1, 2) -> 12.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("div3")) { ob =>
          "correctly compute M / 3" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> (1.0 / 3.0), (2, 2) -> (3.0 / 3.0), (1, 2) -> (4.0 / 3.0))
          }
        }
        .sink[(Int, Int, Double)](Tsv("timestrace")) { ob =>
          "correctly compute M * Tr(M)" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 4.0, (2, 2) -> 12.0, (1, 2) -> 16.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("tracetimes")) { ob =>
          "correctly compute Tr(M) * M" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 4.0, (2, 2) -> 12.0, (1, 2) -> 16.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("divtrace")) { ob =>
          "correctly compute M / Tr(M)" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> (1.0 / 4.0), (2, 2) -> (3.0 / 4.0), (1, 2) -> (4.0 / 4.0))
          }
        }
        .run
        .finish()
    }
  }
  "A Matrix Diagonal job" should {
    TUtil.printStack {
      JobTest(new DiagonalOps(_))
        /* [[1.0 4.0]
       *  [0.0 3.0]]
       */
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("diag-mat")) { ob =>
          "correctly compute diag * matrix" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (1, 2) -> 4.0, (2, 2) -> 9.0)
          }
        }
        .sink[(Int, Double)](Tsv("diag-diag")) { ob =>
          "correctly compute diag * diag" in {
            oneDtoSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (2, 2) -> 9.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("mat-diag")) { ob =>
          "correctly compute matrix * diag" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (1, 2) -> 12.0, (2, 2) -> 9.0)
          }
        }
        .sink[(Int, Double)](Tsv("diag-col")) { ob =>
          "correctly compute diag * col" in {
            ob.toMap shouldBe Map(1 -> 1.0)
          }
        }
        .sink[(Int, Double)](Tsv("row-diag")) { ob =>
          "correctly compute row * diag" in {
            ob.toMap shouldBe Map(1 -> 1.0, 2 -> 12.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Propagation job" should {
    TUtil.printStack {
      JobTest(new PropJob(_))
        /* [[0 1 1],
        *  [0 0 1],
        *  [1 0 0]] = List((0,1,1), (0,2,1), (1,2,1), (2,0,1))
        * [1.0 2.0 4.0] = List((0,1.0), (1,2.0), (2,4.0))
        */
        .source(TypedTsv[(Int, Int, Int)]("graph"), List((0, 1, 1), (0, 2, 1), (1, 2, 1), (2, 0, 1)))
        .source(TypedTsv[(Int, Double)]("row"), List((0, 1.0), (1, 2.0), (2, 4.0)))
        .source(TypedTsv[(Int, Double)]("col"), List((0, 1.0), (1, 2.0), (2, 4.0)))
        .sink[(Int, Double)](Tsv("prop-col")) { ob =>
          "correctly propagate columns" in {
            ob.toMap shouldBe Map(0 -> 6.0, 1 -> 4.0, 2 -> 1.0)
          }
        }
        .sink[(Int, Double)](Tsv("prop-row")) { ob =>
          "correctly propagate rows" in {
            ob.toMap shouldBe Map(0 -> 4.0, 1 -> 1.0, 2 -> 3.0)
          }
        }
        .run
        .finish()
    }
  }

  "A MapWithIndex job" should {
    JobTest(new MatrixMapWithVal(_))
      .source(TypedTsv[(Int, Int, Int)]("graph"), List((0, 1, 1), (1, 1, 3), (0, 2, 1), (1, 2, 1), (2, 0, 1)))
      .source(TypedTsv[(Int, Double)]("row"), List((0, 1.0), (1, 2.0), (2, 4.0)))
      .sink[(Int, Double)](Tsv("first")) { ob =>
        "correctly mapWithIndex on Row" in {
          ob.toMap shouldBe Map(0 -> 1.0)
        }
      }
      .sink[(Int, Int, Int)](Tsv("diag")) { ob =>
        "correctly mapWithIndex on Matrix" in {
          toSparseMat(ob) shouldBe Map((1, 1) -> 3)
        }
      }
      .run
      .finish()
  }

  "A Matrix RowMatProd job" should {
    TUtil.printStack {
      JobTest(new RowMatProd(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Double)](Tsv("rowMatPrd")) { ob =>
          "correctly compute a new row vector" in {
            oneDtoSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (2, 2) -> 16.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Matrix MatColProd job" should {
    TUtil.printStack {
      JobTest(new MatColProd(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Double)](Tsv("matColPrd")) { ob =>
          "correctly compute a new column vector" in {
            oneDtoSparseMat(ob) shouldBe Map((1, 1) -> 1.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Matrix RowRowDiff job" should {
    TUtil.printStack {
      JobTest(new RowRowDiff(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Double)](Tsv("rowRowDiff")) { ob =>
          "correctly subtract row vectors" in {
            oneDtoSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (2, 2) -> 1.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Matrix VctOuterProd job" should {
    TUtil.printStack {
      JobTest(new VctOuterProd(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("outerProd")) { ob =>
          "correctly compute the outer product of a column and row vector" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (1, 2) -> 4.0, (2, 1) -> 4.0, (2, 2) -> 16.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Matrix RowRowSum job" should {
    TUtil.printStack {
      JobTest(new RowRowSum(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Double)](Tsv("rowRowSum")) { ob =>
          "correctly add row vectors" in {
            oneDtoSparseMat(ob) shouldBe Map((1, 1) -> 2.0, (2, 2) -> 8.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Matrix RowRowHad job" should {
    TUtil.printStack {
      JobTest(new RowRowHad(_))
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Double)](Tsv("rowRowHad")) { ob =>
          "correctly compute a Hadamard product of row vectors" in {
            oneDtoSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (2, 2) -> 16.0)
          }
        }
        .run
        .finish()
    }
  }

  "A FilterMatrix job" should {
    TUtil.printStack {
      JobTest(new FilterMatrix(_))
        .source(Tsv("mat1", ('x, 'y, 'v)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0), (2, 1, 2.0)))
        .source(Tsv("mat2", ('x, 'y, 'v)), List((1, 1, 5.0), (2, 2, 9.0)))
        .sink[(Int, Int, Double)](Tsv("removeMatrix")) { ob =>
          "correctly remove elements" in {
            toSparseMat(ob) shouldBe Map((1, 2) -> 4.0, (2, 1) -> 2.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("keepMatrix")) { ob =>
          "correctly keep elements" in {
            toSparseMat(ob) shouldBe Map((1, 1) -> 1.0, (2, 2) -> 3.0)
          }
        }
        .run
        .finish()
    }
  }

  "A KeepRowsCols job" should {
    TUtil.printStack {
      JobTest(new KeepRowsCols(_))
        .source(Tsv("mat1", ('x, 'y, 'v)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0), (2, 1, 2.0)))
        .source(Tsv("col1", ('x, 'v)), List((1, 5.0)))
        .sink[(Int, Int, Double)](Tsv("keepRows")) { ob =>
          "correctly keep row vectors" in {
            toSparseMat(ob) shouldBe Map((1, 2) -> 4.0, (1, 1) -> 1.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("keepCols")) { ob =>
          "correctly keep col vectors" in {
            toSparseMat(ob) shouldBe Map((2, 1) -> 2.0, (1, 1) -> 1.0)
          }
        }
        .run
        .finish()
    }
  }

  "A RemoveRowsCols job" should {
    TUtil.printStack {
      JobTest(new RemoveRowsCols(_))
        .source(Tsv("mat1", ('x, 'y, 'v)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0), (2, 1, 2.0)))
        .source(Tsv("col1", ('x, 'v)), List((1, 5.0)))
        .sink[(Int, Int, Double)](Tsv("removeRows")) { ob =>
          "correctly keep row vectors" in {
            toSparseMat(ob) shouldBe Map((2, 2) -> 3.0, (2, 1) -> 2.0)
          }
        }
        .sink[(Int, Int, Double)](Tsv("removeCols")) { ob =>
          "correctly keep col vectors" in {
            toSparseMat(ob) shouldBe Map((2, 2) -> 3.0, (1, 2) -> 4.0)
          }
        }
        .run
        .finish()
    }
  }

  "A Scalar Row Right job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ScalarRowRight(_))
        .source(Tsv("sca1", ('v)), List(3.0))
        .source(Tsv("row1", ('x, 'v)), List((1, 1.0), (2, 2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("scalarRowRight")) { ob =>
          s"$idx: correctly compute a new row vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("scalarObjRowRight")) { ob =>
          s"$idx: correctly compute a new row vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Scalar Row Left job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ScalarRowLeft(_))
        .source(Tsv("sca1", ('v)), List(3.0))
        .source(Tsv("row1", ('x, 'v)), List((1, 1.0), (2, 2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("scalarRowLeft")) { ob =>
          s"$idx: correctly compute a new row vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("scalarObjRowLeft")) { ob =>
          s"$idx: correctly compute a new row vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Scalar Col Right job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ScalarColRight(_))
        .source(Tsv("sca1", ('v)), List(3.0))
        .source(Tsv("col1", ('x, 'v)), List((1, 1.0), (2, 2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("scalarColRight")) { ob =>
          s"$idx: correctly compute a new col vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("scalarObjColRight")) { ob =>
          s"$idx: correctly compute a new col vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Scalar Col Left job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ScalarColLeft(_))
        .source(Tsv("sca1", ('v)), List(3.0))
        .source(Tsv("col1", ('x, 'v)), List((1, 1.0), (2, 2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("scalarColLeft")) { ob =>
          s"$idx: correctly compute a new col vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("scalarObjColLeft")) { ob =>
          s"$idx: correctly compute a new col vector" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Scalar Diag Right job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ScalarDiagRight(_))
        .source(Tsv("sca1", ('v)), List(3.0))
        .source(Tsv("diag1", ('x, 'v)), List((1, 1.0), (2, 2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("scalarDiagRight")) { ob =>
          s"$idx: correctly compute a new diag matrix" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("scalarObjDiagRight")) { ob =>
          s"$idx: correctly compute a new diag matrix" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Scalar Diag Left job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ScalarDiagLeft(_))
        .source(Tsv("sca1", ('v)), List(3.0))
        .source(Tsv("diag1", ('x, 'v)), List((1, 1.0), (2, 2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("scalarDiagLeft")) { ob =>
          s"$idx: correctly compute a new diag matrix" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("scalarObjDiagLeft")) { ob =>
          s"$idx: correctly compute a new diag matrix" in {
            ob.toMap shouldBe Map(1 -> 3.0, 2 -> 6.0, 3 -> 18.0)
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Col Normalizing job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new ColNormalize(_))
        .source(Tsv("col1", ('x, 'v)), List((1, 1.0), (2, -2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("colLZeroNorm")) { ob =>
          s"$idx: correctly compute a new col vector" in {
            ob.toMap shouldBe Map(1 -> (1.0 / 3.0), 2 -> (-2.0 / 3.0), 3 -> (6.0 / 3.0))
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("colLOneNorm")) { ob =>
          s"$idx: correctly compute a new col vector" in {
            ob.toMap shouldBe Map(1 -> (1.0 / 9.0), 2 -> (-2.0 / 9.0), 3 -> (6.0 / 9.0))
          }
          idx += 1
        }
        .run
        .finish()
    }
  }

  "A Col Diagonal job" should {
    TUtil.printStack {
      "correctly compute the size of the diagonal matrix" in {
        val col = new ColDiagonal(Mode.putMode(new Test(Map.empty), new Args(Map.empty)))
        col.sizeHintTotal shouldBe 100L
      }
    }
  }

  "A Row Normalizing job" should {
    TUtil.printStack {
      var idx = 0
      JobTest(new RowNormalize(_))
        .source(Tsv("row1", ('x, 'v)), List((1, 1.0), (2, -2.0), (3, 6.0)))
        .sink[(Int, Double)](Tsv("rowLZeroNorm")) { ob =>
          s"$idx: correctly compute a new row vector" in {
            ob.toMap shouldBe Map(1 -> (1.0 / 3.0), 2 -> (-2.0 / 3.0), 3 -> (6.0 / 3.0))
          }
          idx += 1
        }
        .sink[(Int, Double)](Tsv("rowLOneNorm")) { ob =>
          s"$idx: correctly compute a new row vector" in {
            ob.toMap shouldBe Map(1 -> (1.0 / 9.0), 2 -> (-2.0 / 9.0), 3 -> (6.0 / 9.0))
          }
          idx += 1
        }
        .run
        .finish()
    }
  }
}
