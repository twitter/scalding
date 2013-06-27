package com.twitter.scalding.mathematics

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck._
import org.scalacheck.Gen._
import com.twitter.scalding._
import Matrix2._
import cascading.flow.FlowDef

object Matrix2Props extends Properties("Matrix2") {

  /**
   * Helper methods used in tests for randomized generations
   */
  def genLeaf(dims: (Long, Long)): (Literal, Long) = {
    val (rows, cols) = dims
    val sparGen = Gen.choose(0.0f, 1.0f)
    val sparsity = sparGen.sample.get
    val rowGen = Gen.choose(1, 1000)
    val nextRows = if (rows <= 0) rowGen.sample.get else rows
    if (cols <= 0) {
      val colGen = Gen.choose(1, 1000)
      val nextCols = colGen.sample.get
      (new Literal(SparseHint(sparsity, nextRows, nextCols)), nextCols)
    } else {
      (new Literal(SparseHint(sparsity, nextRows, cols)), cols)
    }
  }

  def productChainGen(current: Int, target: Int, prevCol: Long, result: List[Literal]): List[Literal] = {
    if (current == target) result
    else {
      val (randomMatrix, cols) = genLeaf((prevCol, 0))
      productChainGen(current + 1, target, cols, result ++ List(randomMatrix))
    }
  }

  def randomProduct(p: Int): Matrix2 = {
    if (p == 1) genLeaf((0, 0))._1
    else {
      val full = productChainGen(0, p, 0, Nil).toIndexedSeq
      generateRandomPlan(0, full.size - 1, full)
    }
  }

  val genNode = for {
    v <- arbitrary[Int]
    p <- Gen.choose(1, 10)
    left <- genFormula
    right <- genFormula
  } yield if (v > 0) randomProduct(p) else Sum(left, right)

  def genFormula: Gen[Matrix2] = oneOf(genNode, genLeaf((0, 0))._1)

  implicit def arbT: Arbitrary[Matrix2] = Arbitrary(genFormula)

  val genProdSeq = for {
    v <- Gen.choose(1, 10)
  } yield productChainGen(0, v, 0, Nil).toIndexedSeq

  implicit def arbSeq: Arbitrary[IndexedSeq[Literal]] = Arbitrary(genProdSeq)

  def generateRandomPlan(i: Int, j: Int, p: IndexedSeq[Literal]): Matrix2 = {
    if (i == j) p(i)
    else {
      val genK = Gen.choose(i, j - 1)
      val k = genK.sample.getOrElse(i)
      val X = generateRandomPlan(i, k, p)
      val Y = generateRandomPlan(k + 1, j, p)
      Product(X, Y)
    }
  }

  property("optimizing an optimized plan does not change it") = forAll { (a: Matrix2) =>
    optimize(a) == optimize(optimize(a)._2)
  }

  /**
   * Function that recursively estimates a cost of a given MatrixFormula / plan.
   * This is the used in the tests for checking whether an optimized plan has
   * a cost <= a randomized plan.
   * The cost estimation of this evaluation should return the same values as the one
   * used in building optimized plans -- this is checked in the tests below.
   * @return resulting cost
   */
  def evaluate(mf: Matrix2): Long = {

    /**
     * This function strips off the formula into a list of independent product chains
     * (i.e. same as matrixFormulaToChains in Prototype, but has Products
     * instead of IndexedSeq[Literal])
     */
    def toProducts(mf: Matrix2): (Option[Product], List[Product]) = {
      mf match {
        case element: Literal => (None, Nil)
        case Sum(left, right) => {
          val (lastLP, leftR) = toProducts(left)
          val (lastRP, rightR) = toProducts(right)
          val total = leftR ++ rightR ++ (if (lastLP.isDefined) List(lastLP.get) else Nil) ++
            (if (lastRP.isDefined) List(lastRP.get) else Nil)
          (None, total)
        }
        case Product(leftp: Literal, rightp: Literal, _) => {
          (Some(Product(leftp, rightp)), Nil)
        }
        case Product(left: Product, right: Literal, _) => {
          val (lastLP, leftR) = toProducts(left)
          if (lastLP.isDefined) (Some(Product(lastLP.get, right)), leftR)
          else (None, leftR)
        }
        case Product(left: Literal, right: Product, _) => {
          val (lastRP, rightR) = toProducts(right)
          if (lastRP.isDefined) (Some(Product(left, lastRP.get)), rightR)
          else (None, rightR)
        }
        case Product(left, right, _) => {
          val (lastLP, leftR) = toProducts(left)
          val (lastRP, rightR) = toProducts(right)
          if (lastLP.isDefined && lastRP.isDefined) {
            (Some(Product(lastLP.get, lastRP.get)), leftR ++ rightR)
          } else {
            val newP = if (lastLP.isDefined) List(lastLP.get) else if (lastRP.isDefined) List(lastRP.get) else Nil
            (None, newP ++ leftR ++ rightR)
          }

        }
      }
    }

    /**
     * This function evaluates a product chain in the same way
     * as the dynamic programming procedure computes cost
     * (optimizeProductChain - computeCosts in Prototype)
     */
    def evaluateProduct(p: Product): Option[(Long, Matrix2, Matrix2)] = {
      p match {
        case Product(left: Literal, right: Literal, _) => {
          Some((left.sizeHint * (left.sizeHint * right.sizeHint)).total.get,
            left, right)
        }
        case Product(left: Literal, right: Product, _) => {
          val (cost, pLeft, pRight) = evaluateProduct(right).get
          Some(cost + (left.sizeHint * (left.sizeHint * pRight.sizeHint)).total.get,
            left, pRight)
        }
        case Product(left: Product, right: Literal, _) => {
          val (cost, pLeft, pRight) = evaluateProduct(left).get
          Some(cost + (pLeft.sizeHint * (pRight.sizeHint * right.sizeHint)).total.get,
            pLeft, right)
        }
        case Product(left: Product, right: Product, _) => {
          val (cost1, p1Left, p1Right) = evaluateProduct(left).get
          val (cost2, p2Left, p2Right) = evaluateProduct(right).get
          Some(cost1 + cost2 + (p1Left.sizeHint * (p1Right.sizeHint * p2Right.sizeHint)).total.get,
            p1Left, p2Right)
        }
        case _ => None
      }
    }

    val (last, productList) = toProducts(mf)
    val products = if (last.isDefined) last.get :: productList else productList
    products.map(p => evaluateProduct(p).get._1).sum
  }

  /**
   * Verifying "evaluate" function - that it does return
   * the same overall costs as what is estimated in the optimization procedure
   */

  property("evaluate function returns the same cost as optimize") = forAll { (a: Matrix2) =>
    optimize(a)._1 == evaluate(optimize(a)._2)
  }

  /**
   * "Proof": the goal property that estimated costs of optimized plans or product chains
   * are less than or equal to costs of randomized equivalent plans or product chains
   */
  property("a cost of an optimized chain of matrix products is <= a random one") = forAll { (a: IndexedSeq[Literal]) =>
    optimizeProductChain(a)._1 <= evaluate(generateRandomPlan(0, a.length - 1, a))
  }

  property("cost of a random plan is <= a random one") = forAll { (a: Matrix2) =>
    optimize(a)._1 <= evaluate(a)
  }

}