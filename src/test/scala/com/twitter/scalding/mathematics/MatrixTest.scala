package com.twitter.scalding.mathematics

import com.twitter.scalding._
import cascading.pipe.joiner._
import org.specs._

object TUtil {
  def printStack( fn: => Unit ) {
    try { fn } catch { case e : Throwable => e.printStackTrace; throw e }
  }
}

class MatrixProd(args : Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1",('x1,'y1,'v1))
    .toMatrix[Int,Int,Double]('x1,'y1,'v1)

  val gram = mat1 * mat1.transpose
  gram.pipe.write(Tsv("product"))
}


class MatrixSum(args : Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1",('x1,'y1,'v1))
    .mapToMatrix('x1,'y1,'v1) { rowColVal : (Int,Int,Double) => rowColVal }
  val mat2 = Tsv("mat2",('x2,'y2,'v2))
    .mapToMatrix('x2,'y2,'v2) { rowColVal : (Int,Int,Double) => rowColVal }

  val sum = mat1 + mat2
  sum.pipe.write(Tsv("sum"))
}

class MatrixSum3(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,(Double, Double, Double)]('x1,'y1,'v1, p1)

  val sum = mat1 + mat1
  sum.pipe.write(Tsv("sum"))
}


class Randwalk(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val mat1L1Norm = mat1.rowL1Normalize
  val randwalk = mat1L1Norm * mat1L1Norm
  randwalk.pipe.write(Tsv("randwalk"))
}

class Cosine(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val matL2Norm = mat1.rowL2Normalize
  val cosine = matL2Norm * matL2Norm.transpose
  cosine.pipe.write(Tsv("cosine"))
}

class Covariance(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val matCentered = mat1.colMeanCentering
  val cov = matCentered * matCentered.transpose
  cov.pipe.write(Tsv("cov"))
}

class VctProd(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val row = mat1.getRow(1)
  val rowProd = row * row.transpose
  rowProd.pipe.write(Tsv("vctProd"))
}

class VctDiv(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val row = mat1.getRow(1).diag
  val row2 = mat1.getRow(2).diag.inverse
  val rowDiv = row * row2
  rowDiv.pipe.write(Tsv("vctDiv"))
}

class ScalarOps(args: Args) extends Job(args) {
  import Matrix._
  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)
  (mat1 * 3.0).pipe.write(Tsv("times3"))
  (mat1 / 3.0).pipe.write(Tsv("div3"))
  (3.0 * mat1).pipe.write(Tsv("3times"))
  // Now with Scalar objects:
  (mat1.trace * mat1).pipe.write(Tsv("tracetimes"))
  (mat1 * mat1.trace).pipe.write(Tsv("timestrace"))
  (mat1 / mat1.trace).pipe.write(Tsv("divtrace"))
}


class MatrixTest extends Specification {
  
  import Dsl._

  def toSparseMat[Row,Col,V](iter : Iterable[(Row,Col,V)]) : Map[(Row,Col),V] = {
    iter.map { it => ((it._1, it._2),it._3) }.toMap
  }

  "A MatrixProd job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.MatrixProd")
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

  "A MatrixSum job, where the Matrix contains tuples as values," should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.MatrixSum3")
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

  "A Matrix Randwalk job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Randwalk")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("randwalk")) { ob =>
        "correctly compute matrix randwalk" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->0.04, (1,2)->0.24, (2,2)->1.0 ))
        }
      }
      .run
      .finish
    }
  }
  "A Matrix Cosine job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Cosine")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("cosine")) { ob =>
        "correctly compute cosine similarity" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (1,2)->0.9701425001453319, (2,1)->0.9701425001453319, (2,2)->1.0 ))
        }
      }
      .run
      .finish
    }
  }
  "A Matrix Covariance job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Covariance")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("cov")) { ob =>
        "correctly compute matrix covariance" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->0.25, (1,2)-> -0.25, (2,1)-> -0.25, (2,2)->0.25 ))
        }
      }
      .run
      .finish
    }
  }
  "A Matrix VctProd job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.VctProd")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[Double](Tsv("vctProd")) { ob =>
        "correctly compute vector inner products" in {
          ob(0) must be_==(17.0)
        }
      }
      .run
      .finish
    }
  }
  "A Matrix VctDiv job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.VctDiv")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("vctDiv")) { ob =>
        "correctly compute vector element-wise division" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((2,2)->1.3333333333333333) )
        }
      }
      .run
      .finish
    }
  }
  "A Matrix ScalarOps job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.ScalarOps")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("times3")) { ob =>
        "correctly compute M * 3" in {
          toSparseMat(ob) must be_==( Map((1,1)->3.0, (2,2)->9.0, (1,2)->12.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("3times")) { ob =>
        "correctly compute 3 * M" in {
          toSparseMat(ob) must be_==( Map((1,1)->3.0, (2,2)->9.0, (1,2)->12.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("div3")) { ob =>
        "correctly compute M / 3" in {
          toSparseMat(ob) must be_==( Map((1,1)->(1.0/3.0), (2,2)->(3.0/3.0), (1,2)->(4.0/3.0)) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("timestrace")) { ob =>
        "correctly compute M * Tr(M)" in {
          toSparseMat(ob) must be_==( Map((1,1)->4.0, (2,2)->12.0, (1,2)->16.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("tracetimes")) { ob =>
        "correctly compute Tr(M) * M" in {
          toSparseMat(ob) must be_==( Map((1,1)->4.0, (2,2)->12.0, (1,2)->16.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("divtrace")) { ob =>
        "correctly compute M / Tr(M)" in {
          toSparseMat(ob) must be_==( Map((1,1)->(1.0/4.0), (2,2)->(3.0/4.0), (1,2)->(4.0/4.0)) )
        }
      }
      .run
      .finish
    }
  }
}
