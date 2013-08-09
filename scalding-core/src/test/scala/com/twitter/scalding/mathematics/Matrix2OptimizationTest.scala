package com.twitter.scalding.mathematics

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck._
import org.scalacheck.Gen._
import org.specs._
import com.twitter.scalding._
import Matrix2._
import cascading.flow.FlowDef
import com.twitter.algebird.Ring
import com.twitter.scalding.IterableSource

/**
 * Unit tests used in development
 * (stronger properties are tested in ScalaCheck tests at the end)
 */
class Matrix2OptimizationSpec extends Specification {
  import Dsl._
  import com.twitter.scalding.Test

  implicit val mode = Test(Map())
  implicit val fd = new FlowDef

  val globM = TypedPipe.from(IterableSource(List((1, 2, 3.0), (2, 2, 4.0))))

  implicit val ring: Ring[Double] = Ring.doubleRing
  implicit val ord1: Ordering[Int] = Ordering.Int
  implicit val ord2: Ordering[(Int, Int)] = Ordering.Tuple2[Int, Int]

  def literal(tpipe: TypedPipe[(Int, Int, Double)], sizeHint: SizeHint): MatrixLiteral[Any, Any, Double] = MatrixLiteral(tpipe, sizeHint).asInstanceOf[MatrixLiteral[Any, Any, Double]]
  def product(left: Matrix2[Any, Any, Double], right: Matrix2[Any, Any, Double], optimal: Boolean = false): Product[Any, Any, Any, Double] = Product(left, right, optimal, ring)
  def sum(left: Matrix2[Any, Any, Double], right: Matrix2[Any, Any, Double]): Sum[Any, Any, Double] = Sum(left, right, ring)

  /**
   * Values used in tests
   */
  // ((A1(A2 A3))((A4 A5) A6)
  val optimizedPlan = product(
    product(literal(globM, FiniteHint(30, 35)),
      product(literal(globM, FiniteHint(35, 15)),
        literal(globM, FiniteHint(15, 5)), true), true),
    product(
      product(literal(globM, FiniteHint(5, 10)),
        literal(globM, FiniteHint(10, 20)), true),
      literal(globM, FiniteHint(20, 25)), true), true)

  val optimizedPlanCost = 1300 // originally 15125.0

  // A1(A2(A3(A4(A5 A6))))
  val unoptimizedPlan = product(literal(globM, FiniteHint(30, 35)),
    product(literal(globM, FiniteHint(35, 15)),
      product(literal(globM, FiniteHint(15, 5)),
        product(literal(globM, FiniteHint(5, 10)),
          product(literal(globM, FiniteHint(10, 20)), literal(globM, FiniteHint(20, 25)))))))

  val simplePlan = product(literal(globM, FiniteHint(30, 35)), literal(globM, FiniteHint(35, 25)), true)

  val simplePlanCost = 750 //originally 26250

  val combinedUnoptimizedPlan = sum(unoptimizedPlan, simplePlan)

  val combinedOptimizedPlan = sum(optimizedPlan, simplePlan)

  val combinedOptimizedPlanCost = optimizedPlanCost + simplePlanCost

  // A1 * (A2 * (A3 * ( A4 + A4 ) * (A5 * (A6))))

  val unoptimizedGlobalPlan = product(literal(globM, FiniteHint(30, 35)),
    product(literal(globM, FiniteHint(35, 15)),
      product(literal(globM, FiniteHint(15, 5)),
        product(sum(literal(globM, FiniteHint(5, 10)), literal(globM, FiniteHint(5, 10))),
          product(literal(globM, FiniteHint(10, 20)), literal(globM, FiniteHint(20, 25)))))))
          
  // ((A1(A2 A3))(((A4 + A4) A5) A6)
  val optimizedGlobalPlan = product(
    product(literal(globM, FiniteHint(30, 35)),
      product(literal(globM, FiniteHint(35, 15)),
        literal(globM, FiniteHint(15, 5)), true), true),
    product(
      product(sum(literal(globM, FiniteHint(5, 10)), literal(globM, FiniteHint(5, 10))),
        literal(globM, FiniteHint(10, 20)), true),
      literal(globM, FiniteHint(20, 25)), true), true)

  val productSequence = IndexedSeq(literal(globM, FiniteHint(30, 35)), literal(globM, FiniteHint(35, 15)),
    literal(globM, FiniteHint(15, 5)), literal(globM, FiniteHint(5, 10)), literal(globM, FiniteHint(10, 20)),
    literal(globM, FiniteHint(20, 25)))

  val combinedSequence = List(IndexedSeq(literal(globM, FiniteHint(30, 35)), literal(globM, FiniteHint(35, 15)),
    literal(globM, FiniteHint(15, 5)), literal(globM, FiniteHint(5, 10)), literal(globM, FiniteHint(10, 20)),
    literal(globM, FiniteHint(20, 25))), IndexedSeq(literal(globM, FiniteHint(30, 35)), literal(globM, FiniteHint(35, 25))))

  val planWithSum = product(literal(globM, FiniteHint(30, 35)), sum(literal(globM, FiniteHint(35, 25)), literal(globM, FiniteHint(35, 25))), true)

  "Matrix multiplication chain optimization" should {
    "handle a single matrix" in {
      val p = IndexedSeq(literal(globM, FiniteHint(30, 35)))
      val result = optimizeProductChain(p, Some(ring))
      (result == (0, literal(globM, FiniteHint(30, 35)))) must beTrue
    }
    "handle two matrices" in {
      val p = IndexedSeq(literal(globM, FiniteHint(30, 35)), literal(globM, FiniteHint(35, 25)))
      val result = optimizeProductChain(p, Some(ring))
      ((simplePlanCost, simplePlan) == result) must beTrue
    }
    "handle an example with 6 matrices" in {
      val result = optimizeProductChain(productSequence, Some(ring))

      ((optimizedPlanCost, optimizedPlan) == result) must beTrue
    }

    "not change an optimized plan" in {
      ((optimizedPlanCost, optimizedPlan) == optimize(optimizedPlan)) must beTrue
    }

    "change an unoptimized plan" in {
      ((optimizedPlanCost, optimizedPlan) == optimize(unoptimizedPlan)) must beTrue
    }

    "handle an optimized plan with sum" in {
      ((combinedOptimizedPlanCost, combinedOptimizedPlan) == optimize(combinedOptimizedPlan)) must beTrue
    }

    "handle an unoptimized plan with sum" in {
      ((combinedOptimizedPlanCost, combinedOptimizedPlan) == optimize(combinedUnoptimizedPlan)) must beTrue
    }

    "not break A*(B+C)" in {
      (planWithSum == optimize(planWithSum)._2) must beTrue
    }
    
    "handle an unoptimized global plan" in {
      (optimizedGlobalPlan == optimize(unoptimizedGlobalPlan)._2) must beTrue
    }

    "handle an optimized global plan" in {
      (optimizedGlobalPlan == optimize(optimizedGlobalPlan)._2) must beTrue
    }    
    
  }
}

object Matrix2Props extends Properties("Matrix2") {
  import com.twitter.scalding.Test

  implicit val mode = Test(Map())
  implicit val fd = new FlowDef
  val globM = TypedPipe.from(IterableSource(List((1, 2, 3.0), (2, 2, 4.0))))

  implicit val ring: Ring[Double] = Ring.doubleRing
  implicit val ord1: Ordering[Int] = Ordering.Int

  def literal(tpipe: TypedPipe[(Int, Int, Double)], sizeHint: SizeHint): MatrixLiteral[Any, Any, Double] = MatrixLiteral(tpipe, sizeHint).asInstanceOf[MatrixLiteral[Any, Any, Double]]
  def product(left: Matrix2[Any, Any, Double], right: Matrix2[Any, Any, Double], optimal: Boolean = false): Product[Any, Any, Any, Double] = Product(left, right, optimal, ring)
  def sum(left: Matrix2[Any, Any, Double], right: Matrix2[Any, Any, Double]): Sum[Any, Any, Double] = Sum(left, right, ring)

  /**
   * Helper methods used in tests for randomized generations
   */
  def genLeaf(dims: (Long, Long)): (MatrixLiteral[Any, Any, Double], Long) = {
    val (rows, cols) = dims
    val sparGen = Gen.choose(0.0f, 1.0f)
    val sparsity = sparGen.sample.get
    val rowGen = Gen.choose(1, 1000)
    val nextRows = if (rows <= 0) rowGen.sample.get else rows
    if (cols <= 0) {
      val colGen = Gen.choose(1, 1000)
      val nextCols = colGen.sample.get
      (literal(globM, SparseHint(sparsity, nextRows, nextCols)), nextCols)
    } else {
      (literal(globM, SparseHint(sparsity, nextRows, cols)), cols)
    }
  }

  def productChainGen(current: Int, target: Int, prevCol: Long, result: List[MatrixLiteral[Any, Any, Double]]): List[MatrixLiteral[Any, Any, Double]] = {
    if (current == target) result
    else {
      val (randomMatrix, cols) = genLeaf((prevCol, 0))
      productChainGen(current + 1, target, cols, result ++ List(randomMatrix))
    }
  }

  def randomProduct(p: Int): Matrix2[Any, Any, Double] = {
    if (p == 1) genLeaf((0, 0))._1
    else {
      val full = productChainGen(0, p, 0, Nil).toIndexedSeq
      generateRandomPlan(0, full.size - 1, full)
    }
  }

  def genNode(depth: Int) = for {
    v <- arbitrary[Int]
    p <- Gen.choose(1, 10)
    left <- genFormula(depth + 1)
    right <- genFormula(depth + 1)
  } yield if (depth > 5) randomProduct(p) else (if (v > 0) randomProduct(p) else Sum(left, right, ring))

  def genFormula(depth: Int): Gen[Matrix2[Any, Any, Double]] = if (depth > 5) genLeaf((0, 0))._1 else (oneOf(genNode(depth + 1), genLeaf((0, 0))._1))

  implicit def arbT: Arbitrary[Matrix2[Any, Any, Double]] = Arbitrary(genFormula(0))

  val genProdSeq = for {
    v <- Gen.choose(1, 10)
  } yield productChainGen(0, v, 0, Nil).toIndexedSeq

  implicit def arbSeq: Arbitrary[IndexedSeq[MatrixLiteral[Any, Any, Double]]] = Arbitrary(genProdSeq)

  def generateRandomPlan(i: Int, j: Int, p: IndexedSeq[MatrixLiteral[Any, Any, Double]]): Matrix2[Any, Any, Double] = {
    if (i == j) p(i)
    else {
      val genK = Gen.choose(i, j - 1)
      val k = genK.sample.getOrElse(i)
      val X = generateRandomPlan(i, k, p)
      val Y = generateRandomPlan(k + 1, j, p)
      Product(X, Y, false, ring)
    }
  }

  /**
   * Function that recursively estimates a cost of a given MatrixFormula / plan.
   * This is the used in the tests for checking whether an optimized plan has
   * a cost <= a randomized plan.
   * The cost estimation of this evaluation should return the same values as the one
   * used in building optimized plans -- this is checked in the tests below.
   * @return resulting cost
   */
  def evaluate(mf: Matrix2[Any, Any, Double]): Long = {

    /**
     * This function strips off the formula into a list of independent product chains
     * (i.e. same as matrixFormulaToChains in Prototype, but has Products
     * instead of IndexedSeq[Literal])
     */
    def toProducts(mf: Matrix2[Any, Any, Double]): (Option[Product[Any, Any, Any, Double]], List[Product[Any, Any, Any, Double]]) = {
      mf match {
        case element: MatrixLiteral[Any, Any, Double] => (None, Nil)
        case Sum(left, right, _) => {
          val (lastLP, leftR) = toProducts(left)
          val (lastRP, rightR) = toProducts(right)
          val total = leftR ++ rightR ++ (if (lastLP.isDefined) List(lastLP.get) else Nil) ++
            (if (lastRP.isDefined) List(lastRP.get) else Nil)
          (None, total)
        }
        case Product(leftp: MatrixLiteral[Any, Any, Double], rightp: MatrixLiteral[Any, Any, Double], _, _) => {
          (Some(Product(leftp, rightp, false, ring)), Nil)
        }
        case Product(left: Product[Any, Any, Any, Double], right: MatrixLiteral[Any, Any, Double], _, _) => {
          val (lastLP, leftR) = toProducts(left)
          if (lastLP.isDefined) (Some(Product(lastLP.get, right, false, ring)), leftR)
          else (None, leftR)
        }
        case Product(left: MatrixLiteral[Any, Any, Double], right: Product[Any, Any, Any, Double], _, _) => {
          val (lastRP, rightR) = toProducts(right)
          if (lastRP.isDefined) (Some(Product(left, lastRP.get, false, ring)), rightR)
          else (None, rightR)
        }
        case Product(left, right, _, _) => {
          val (lastLP, leftR) = toProducts(left)
          val (lastRP, rightR) = toProducts(right)
          if (lastLP.isDefined && lastRP.isDefined) {
            (Some(Product(lastLP.get, lastRP.get, false, ring)), leftR ++ rightR)
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
    def evaluateProduct(p: Matrix2[Any, Any, Double]): Option[(Long, Matrix2[Any, Any, Double], Matrix2[Any, Any, Double])] = {
      p match {
        case Product(left: MatrixLiteral[Any, Any, Double], right: MatrixLiteral[Any, Any, Double], _, _) => {
          Some((left.sizeHint * (left.sizeHint * right.sizeHint)).total.get,
            left, right)
        }
        case Product(left: MatrixLiteral[Any, Any, Double], right: Product[Any, Any, Any, Double], _, _) => {
          val (cost, pLeft, pRight) = evaluateProduct(right).get
          Some(cost + (left.sizeHint * (left.sizeHint * pRight.sizeHint)).total.get,
            left, pRight)
        }
        case Product(left: Product[Any, Any, Any, Double], right: MatrixLiteral[Any, Any, Double], _, _) => {
          val (cost, pLeft, pRight) = evaluateProduct(left).get
          Some(cost + (pLeft.sizeHint * (pRight.sizeHint * right.sizeHint)).total.get,
            pLeft, right)
        }
        case Product(left: Matrix2[Any, Any, Double], right: Matrix2[Any, Any, Double], _, _) => {
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

  // ScalaCheck properties
  /**
   * Verifying "evaluate" function - that it does return
   * the same overall costs as what is estimated in the optimization procedure
   */
  property("evaluate function returns the same cost as optimize") = forAll { (a: Matrix2[Any, Any, Double]) =>
    optimize(a)._1 == evaluate(optimize(a)._2)
  }

  /**
   * "Proof": the goal property that estimated costs of optimized plans or product chains
   * are less than or equal to costs of randomized equivalent plans or product chains
   */
  property("a cost of an optimized chain of matrix products is <= a random one") = forAll { (a: IndexedSeq[MatrixLiteral[Any, Any, Double]]) =>
    optimizeProductChain(a, Some(ring))._1 <= evaluate(generateRandomPlan(0, a.length - 1, a))
  }

  property("cost of a random plan is <= a random one") = forAll { (a: Matrix2[Any, Any, Double]) =>
    optimize(a)._1 <= evaluate(a)
  }

  /**
   * Sanity check
   */
  property("optimizing an optimized plan does not change it") = forAll { (a: Matrix2[Any, Any, Double]) =>
    optimize(a) == optimize(optimize(a)._2)
  }

}