package com.twitter.scalding.mathematics

import com.twitter.scalding._
import cascading.pipe.joiner._
import org.specs._
import com.twitter.algebird.{Ring,Group}

class Matrix2Sum(args: Args) extends Job(args) {

  import Matrix2._
  import cascading.pipe.Pipe
  import cascading.tuple.Fields
  import com.twitter.scalding.TDsl._
  
  val p1: Pipe = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val tp1 = p1.toTypedPipe[(Int, Int, Double)](('x1, 'y1, 'v1))
  val mat1 = MatrixLiteral(tp1, NoClue)

  val p2 = Tsv("mat2", ('x2, 'y2, 'v2)).read
  val tp2 = p2.toTypedPipe[(Int, Int, Double)](('x2, 'y2, 'v2))
  val mat2 = MatrixLiteral(tp2, NoClue)
  
  val sum = mat1 + mat2
  sum.toTypedPipe.toPipe(('x1, 'y1, 'v1)).write(Tsv("sum"))
}

class Matrix2Sum3(args: Args) extends Job(args) {

  import Matrix2._
  import cascading.pipe.Pipe
  import cascading.tuple.Fields
  import com.twitter.scalding.TDsl._
  
  val p1: Pipe = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val tp1 = p1.toTypedPipe[(Int, Int, (Double, Double, Double))](('x1, 'y1, 'v1))
  val mat1 = MatrixLiteral(tp1, NoClue)

  val sum = mat1 + mat1
  sum.toTypedPipe.toPipe(('x1, 'y1, 'v1)).write(Tsv("sum"))
}

class Matrix2Prod(args: Args) extends Job(args) {

  import Matrix2._
  import cascading.pipe.Pipe
  import cascading.tuple.Fields
  import com.twitter.scalding.TDsl._

  val p1: Pipe = Tsv("mat1", ('x1, 'y1, 'v1)).read
  val tp1 = p1.toTypedPipe[(Int, Int, Double)](('x1, 'y1, 'v1))
  val mat1 = MatrixLiteral(tp1, NoClue)

  val gram = mat1 * mat1.transpose
  gram.toTypedPipe.toPipe(('x1, 'y1, 'v1)).write(Tsv("product"))
}

class Matrix2Test extends Specification {
  noDetailedDiffs() // For scala 2.9
  import Dsl._

  def toSparseMat[Row, Col, V](iter: Iterable[(Row, Col, V)]): Map[(Row, Col), V] = {
    iter.map { it => ((it._1, it._2), it._3) }.toMap
  }
  def oneDtoSparseMat[Idx, V](iter: Iterable[(Idx, V)]): Map[(Idx, Idx), V] = {
    iter.map { it => ((it._1, it._1), it._2) }.toMap
  }

  "A MatrixSum job" should {
    TUtil.printStack {
      JobTest("com.twitter.scalding.mathematics.MatrixSum")
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .source(Tsv("mat2", ('x2, 'y2, 'v2)), List((1, 3, 3.0), (2, 1, 8.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("sum")) { ob =>
          "correctly compute sums" in {
            val pMap = toSparseMat(ob)
            pMap must be_==(Map((1, 1) -> 1.0, (1, 2) -> 8.0, (1, 3) -> 3.0, (2, 1) -> 8.0, (2, 2) -> 3.0))
          }
        }
        .run
        .finish
    }
  }

  "A Matrix2Sum3 job, where the Matrix contains tuples as values," should {
    TUtil.printStack {
      JobTest("com.twitter.scalding.mathematics.Matrix2Sum3")
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1,1,(1.0, 3.0, 5.0)),(2,2,(3.0, 2.0, 1.0)),(1,2,(4.0, 5.0, 2.0))))
        .sink[(Int, Int, (Double, Double, Double))](Tsv("sum")) { ob =>
          "correctly compute sums" in {
            val pMap = toSparseMat(ob)
            pMap must be_==(Map((1,1)->(2.0, 6.0, 10.0), (2,2)->(6.0, 4.0, 2.0), (1,2)->(8.0, 10.0, 4.0)))
          }
        }
        .run
        .finish
    }
  }

  "A Matrix2Prod job" should {
    TUtil.printStack {
      JobTest("com.twitter.scalding.mathematics.Matrix2Prod")
        .source(Tsv("mat1", ('x1, 'y1, 'v1)), List((1, 1, 1.0), (2, 2, 3.0), (1, 2, 4.0)))
        .sink[(Int, Int, Double)](Tsv("product")) { ob =>
          "correctly compute products" in {
            val pMap = toSparseMat(ob)
            pMap must be_==(Map((1, 1) -> 17.0, (1, 2) -> 12.0, (2, 1) -> 12.0, (2, 2) -> 9.0))
          }
        }
        .run
        .finish
    }
  }

}
