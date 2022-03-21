/*
 Copyright 2014 Twitter, Inc.
 Copyright 2017 Stripe, Inc.

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

package com.twitter.scalding.dagon

import org.scalacheck.Prop._
import org.scalacheck.{Gen, Prop, Properties}

import ScalaVersionCompat.ieeeDoubleOrdering

object DagTests extends Properties("Dag") {

  /*
   * Here we test with a simple algebra optimizer
   */

  sealed trait Formula[T] { // we actually will ignore T
    def evaluate: Int
    def closure: Set[Formula[T]]

    def inc(n: Int): Formula[T] = Inc(this, n)
    def +(that: Formula[T]): Formula[T] = Sum(this, that)
    def *(that: Formula[T]): Formula[T] = Product(this, that)

    override def equals(that: Any) = that match {
      case thatF: Formula[_] => eqFn(RefPair(this, thatF))
      case _ => false
    }
  }

  object Formula {
    def apply(n: Int): Formula[Unit] = Constant(n)

    def inc[T](by: Int): Formula[T] => Formula[T] = Inc(_, by)
    def sum[T]: (Formula[T], Formula[T]) => Formula[T] = Sum(_, _)
    def product[T]: (Formula[T], Formula[T]) => Formula[T] = Product(_, _)
  }

  case class Constant[T](override val evaluate: Int) extends Formula[T] {
    def closure = Set(this)
  }
  case class Inc[T](in: Formula[T], by: Int) extends Formula[T] {
    override val hashCode = (getClass, in, by).hashCode
    def evaluate = in.evaluate + by
    def closure = in.closure + this
  }
  case class Sum[T](left: Formula[T], right: Formula[T]) extends Formula[T] {
    override val hashCode = (getClass, left, right).hashCode
    def evaluate = left.evaluate + right.evaluate
    def closure = (left.closure ++ right.closure) + this
  }
  case class Product[T](left: Formula[T], right: Formula[T]) extends Formula[T] {
    override val hashCode = (getClass, left, right).hashCode
    def evaluate = left.evaluate * right.evaluate
    def closure = (left.closure ++ right.closure) + this
  }

  def eqFn: Function[RefPair[Formula[_], Formula[_]], Boolean] =
    Memoize.function[RefPair[Formula[_], Formula[_]], Boolean] {
      case (pair, _) if pair.itemsEq => true
      case (RefPair(Constant(a), Constant(b)), _) => a == b
      case (RefPair(Inc(ia, ca), Inc(ib, cb)), rec) => (ca == cb) && rec(RefPair(ia, ib))
      case (RefPair(Sum(lefta, leftb), Sum(righta, rightb)), rec) =>
        rec(RefPair(lefta, righta)) && rec(RefPair(leftb, rightb))
      case (RefPair(Product(lefta, leftb), Product(righta, rightb)), rec) =>
        rec(RefPair(lefta, righta)) && rec(RefPair(leftb, rightb))
      case other => false
    }

  def testRule[T](start: Formula[T], expected: Formula[T], rule: Rule[Formula]): Prop = {
    val got = Dag.applyRule(start, toLiteral, rule)
    (got == expected) :| s"$got == $expected"
  }

  def genForm: Gen[Formula[Int]] =
    Gen.frequency((1, genProd), (1, genSum), (4, genInc), (4, genConst))

  def genConst: Gen[Formula[Int]] = Gen.chooseNum(Int.MinValue, Int.MaxValue).map(Constant(_))

  def genInc: Gen[Formula[Int]] =
    for {
      by <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      f <- Gen.lzy(genForm)
    } yield Inc(f, by)

  def genSum: Gen[Formula[Int]] =
    for {
      left <- Gen.lzy(genForm)
      // We have to make dags, so select from the closure of left sometimes
      right <- Gen.oneOf(genForm, Gen.oneOf(left.closure.toSeq))
    } yield Sum(left, right)

  def genProd: Gen[Formula[Int]] =
    for {
      left <- Gen.lzy(genForm)
      // We have to make dags, so select from the closure of left sometimes
      right <- Gen.oneOf(genForm, Gen.oneOf(left.closure.toSeq))
    } yield Product(left, right)

  /**
   * Here we convert our dag nodes into Literal[Formula, T]
   */
  def toLiteral: FunctionK[Formula, Literal[Formula, ?]] =
    Memoize.functionK[Formula, Literal[Formula, ?]](
      new Memoize.RecursiveK[Formula, Literal[Formula, ?]] {
        def toFunction[T] = {
          case (c @ Constant(_), _) => Literal.Const(c)
          case (Inc(in, by), f) => Literal.Unary(f(in), Formula.inc(by))
          case (Sum(lhs, rhs), f) => Literal.Binary(f(lhs), f(rhs), Formula.sum)
          case (Product(lhs, rhs), f) => Literal.Binary(f(lhs), f(rhs), Formula.product)
        }
      })

  /**
   * Inc(Inc(a, b), c) = Inc(a, b + c)
   */
  object CombineInc extends Rule[Formula] {
    def apply[T](on: Dag[Formula]) = {
      case Inc(i @ Inc(a, b), c) if on.fanOut(i) == 1 => Some(Inc(a, b + c))
      case _ => None
    }
  }

  object RemoveInc extends PartialRule[Formula] {
    def applyWhere[T](on: Dag[Formula]) = {
      case Inc(f, by) => Sum(f, Constant(by))
    }
  }

  /**
   * We should be able to totally evaluate these formulas
   */
  object EvaluationRule extends Rule[Formula] {
    def apply[T](on: Dag[Formula]) = {
      case Sum(Constant(a), Constant(b)) => Some(Constant(a + b))
      case Product(Constant(a), Constant(b)) => Some(Constant(a * b))
      case Inc(Constant(a), b) => Some(Constant(a + b))
      case _ => None
    }
  }

  @annotation.tailrec
  final def fib[A](a0: A, a1: A, n: Int)(fn: (A, A) => A): A =
    if (n <= 0) a0
    else if (n == 1) a1
    else fib(a1, fn(a0, a1), n - 1)(fn)

  def timeit[A](a: => A): (Double, A) = {
    val start = System.nanoTime()
    val res = a
    val end = System.nanoTime()
    ((end - start).toDouble, res)
  }

  //This is a bit noisey due to timing, but often passes
  property("Evaluation is at most n^(3.0)") = {
     def fibFormula(n: Int): Formula[Unit] = fib(Formula(1), Formula(1), n)(Sum(_, _))

    def runit(n: Int): (Double, Int) =
      timeit(Dag.applyRule(fibFormula(n), toLiteral, EvaluationRule)) match {
        case (t, Constant(res)) => (t, res)
        case (_, other) => sys.error(s"unexpected result: $other")
      }

     def check = {
       val (t10, res10) = runit(10)
       val (t20, res20) = runit(20)
       val (t40, res40) = runit(40)
       val (t80, res80) = runit(80)
       val (t160, res160) = runit(160)
       // if this is polynomial = t(n) ~ Cn^k, so t(20)/t(10) == t(40)/t(20) == 2^k
       val k = List(t160/t80, t80/t40, t40/t20, t20/t10).map(math.log(_)/math.log(2.0)).max
       println(s"${t10}, ${t20}, ${t40}, ${t80}, ${t160}, $k")
       (res10 == fib(1, 1, 10)(_ + _)) &&
       (res20 == fib(1, 1, 20)(_ + _)) &&
       (res40 == fib(1, 1, 40)(_ + _)) &&
       (res80 == fib(1, 1, 80)(_ + _)) &&
       (res160 == fib(1, 1, 160)(_ + _)) &&
       (k < 3.0) // without properly memoized equality checks, this rule becomes exponential
     }
     check || check || check || check // try 4 times if needed to warm up the jit
   }

  //Check the Node[T] <=> Id[T] is an Injection for all nodes reachable from the root

  property("toLiteral/Literal.evaluate is a bijection") = forAll(genForm) { form =>
    toLiteral.apply(form).evaluate == form
  }

  property("Going to Dag round trips") = forAll(genForm) { form =>
    val (dag, id) = Dag(form, toLiteral)
    dag.evaluate(id) == form
  }

  property("CombineInc does not change results") = forAll(genForm) { form =>
    val simplified = Dag.applyRule(form, toLiteral, CombineInc)
    form.evaluate == simplified.evaluate
  }

  property("RemoveInc removes all Inc") = forAll(genForm) { form =>
    val noIncForm = Dag.applyRule(form, toLiteral, RemoveInc)
    def noInc(f: Formula[Int]): Boolean = f match {
      case Constant(_) => true
      case Inc(_, _) => false
      case Sum(l, r) => noInc(l) && noInc(r)
      case Product(l, r) => noInc(l) && noInc(r)
    }
    noInc(noIncForm) && (noIncForm.evaluate == form.evaluate)
  }

  // The normal Inc gen recursively calls the general dag Generator
  def genChainInc: Gen[Formula[Int]] =
    for {
      by <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      chain <- genChain
    } yield Inc(chain, by)

  def genChain: Gen[Formula[Int]] = Gen.frequency((1, genConst), (3, genChainInc))

  property("CombineInc compresses linear Inc chains") = forAll(genChain) { chain =>
    Dag.applyRule(chain, toLiteral, CombineInc) match {
      case Constant(n) => true
      case Inc(Constant(n), b) => true
      case _ => false // All others should have been compressed
    }
  }

  property("EvaluationRule totally evaluates") = forAll(genForm) { form =>
    testRule(form, Constant(form.evaluate), EvaluationRule)
  }

  property("Crush down explicit diamond") = forAll { (xs0: List[Int], ys0: List[Int]) =>
    val a = Formula(123)

    // ensure that we won't ever use the same constant on the LHS and RHS
    // because we want all our inc nodes to fan out to only one other node.
    def munge(xs: List[Int]): List[Int] = xs.take(10).map(_ % 10)
    val (xs, ys) = (munge(0 :: xs0), munge(0 :: ys0).map(_ + 1000))
    val (x, y) = (xs.sum, ys.sum)

    val complex = xs.foldLeft(a)(_ inc _) + ys.foldLeft(a)(_ inc _)
    val expected = a.inc(x) + a.inc(y)
    testRule(complex, expected, CombineInc)
  }

  property("all tails have fanOut of 1") = forAll { (n1: Int, ns: List[Int]) =>
    // Make sure we have a set of distinct nodes
    val tails = (n1 :: ns).zipWithIndex.map { case (i, idx) => Formula(i).inc(idx) }

    val (dag, roots) =
      tails.foldLeft((Dag.empty[Formula](toLiteral), Set.empty[Id[_]])) {
        case ((d, s), f) =>
          val (dnext, id) = d.addRoot(f)
          (dnext, s + id)
      }

    roots.forall(dag.fanOut(_) == 1)
  }

  property("depth is non-decreasing further down the graph") =
    forAll(genForm) { form =>
      val (dag, id) = Dag(form, toLiteral)

      import dag.depthOf

      val di = dag.depthOfId(id)
      val df = depthOf(form)
      val prop1 = di.isDefined
      val prop2 = di == df

      def prop3 = form match {
        case Constant(_) => di == Some(0)
        case Inc(a, _) =>
          (di.get == (depthOf(a).get + 1))
        case Sum(a, b) =>
          (di.get == (depthOf(a).get + 1)) || (di.get == (depthOf(b).get + 1))
        case Product(a, b) =>
          (di.get == (depthOf(a).get + 1)) || (di.get == (depthOf(b).get + 1))
      }

      prop1 && prop2 && prop3
    }
}
