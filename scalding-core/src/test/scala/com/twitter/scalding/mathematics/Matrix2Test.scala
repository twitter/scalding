package com.twitter.scalding.mathematics

import com.twitter.scalding._
import cascading.pipe.joiner._
import org.specs._
import com.twitter.algebird.Group

class Matrix2Sum3(args : Args) extends Job(args) {

  import Matrix2._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Literal[Int,Int,(Double, Double, Double)](('x1,'y1,'v1), p1, NoClue)

  val sum = mat1 + mat1
  sum.tpipe.toPipe(('x1,'y1,'v1)).write(Tsv("sum"))
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

  "A Matrix2Sum job, where the Matrix contains tuples as values," should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Matrix2Sum3")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,(1.0, 3.0, 5.0)),(2,2,(3.0, 2.0, 1.0)),(1,2,(4.0, 5.0, 2.0))))
      .sink[(Int,Int,(Double, Double, Double))](Tsv("sum")) { ob =>
        "correctly compute sums" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->(2.0, 6.0, 10.0), (2,2)->(6.0, 4.0, 2.0), (1,2)->(8.0, 10.0, 4.0)))
        }
      }
      .run
      .finish
    }
  }



}
