package com.twitter.scalding.mathematics

import com.twitter.scalding._
import cascading.pipe.joiner._
import org.specs._
import com.twitter.algebird.Group


class Matrix2Sum(args : Args) extends Job(args) {

  import Matrix2._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Literal(('x1,'y1,'v1), p1, NoClue)

  val p2 = Tsv("mat2",('x2,'y2,'v2)).read
  val mat2 = new Literal(('x2,'y2,'v2), p2, NoClue)  
  
  val sum = mat1 + mat2
  sum.tpipe.get.toPipe(('x1,'y1,'v1)).write(Tsv("sum"))
}

class Matrix2SumSame(args : Args) extends Job(args) {

  import Matrix2._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Literal(('x1,'y1,'v1), p1, NoClue)

  val sum = mat1 + mat1
  sum.tpipe.get.toPipe(('x1,'y1,'v1)).write(Tsv("sum"))
}

class Matrix2Prod(args : Args) extends Job(args) {

  import Matrix2._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Literal(('x1,'y1,'v1), p1, NoClue)

  val gram = mat1 * mat1.transpose
  gram.tpipe.get.toPipe(('x1,'y1,'v1)).write(Tsv("product"))
}


class Matrix2Test extends Specification {
  noDetailedDiffs() // For scala 2.9
  import Dsl._

  def toSparseMat[Row,Col,V](iter : Iterable[(Row,Col,V)]) : Map[(Row,Col),V] = {
    iter.map { it => ((it._1, it._2),it._3) }.toMap
  }
  def oneDtoSparseMat[Idx,V](iter : Iterable[(Idx,V)]) : Map[(Idx,Idx),V] = {
    iter.map { it => ((it._1, it._1), it._2) }.toMap
  }

  "A MatrixSum job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.MatrixSum")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .source(Tsv("mat2",('x2,'y2,'v2)), List((1,3,3.0),(2,1,8.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("sum")) { ob =>
        "correctly compute sums" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (1,2)->8.0, (1,3)->3.0, (2,1)->8.0, (2,2)->3.0))
        }
      }
      .run
      .finish
    }
  }  
  
  "A Matrix2SumSame job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Matrix2SumSame")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,(Double, Double, Double))](Tsv("sum")) { ob =>
        "correctly compute sums" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->2.0, (2,2)->6.0, (1,2)->8.0))
        }
      }
      .run
      .finish
    }
  }

  "A Matrix2Prod job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Matrix2Prod")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("product")) { ob =>
        "correctly compute products" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->17.0, (1,2)->12.0, (2,1)->12.0, (2,2)->9.0))
        }
      }
      .run
      .finish
    }
  }

}
