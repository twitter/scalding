/*
 Copyright 2014 Twitter, Inc.

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

package com.twitter.scalding.graph

import org.scalacheck.Prop._
import org.scalacheck.{ Gen, Properties }

object ExpressionDagTests extends Properties("ExpressionDag") {
  /*
   * Here we test with a simple algebra optimizer
   */

  sealed trait Formula[T] { // we actually will ignore T
    def evaluate: Int
    def closure: Set[Formula[T]]
  }
  case class Constant[T](override val evaluate: Int) extends Formula[T] {
    def closure = Set(this)
  }
  case class Inc[T](in: Formula[T], by: Int) extends Formula[T] {
    def evaluate = in.evaluate + by
    def closure = in.closure + this
  }
  case class Sum[T](left: Formula[T], right: Formula[T]) extends Formula[T] {
    def evaluate = left.evaluate + right.evaluate
    def closure = (left.closure ++ right.closure) + this
  }
  case class Product[T](left: Formula[T], right: Formula[T]) extends Formula[T] {
    def evaluate = left.evaluate * right.evaluate
    def closure = (left.closure ++ right.closure) + this
  }

  def genForm: Gen[Formula[Int]] = Gen.frequency((1, genProd),
    (1, genSum),
    (4, genInc),
    (4, genConst))

  def genConst: Gen[Formula[Int]] = Gen.chooseNum(Int.MinValue, Int.MaxValue).map(Constant(_))
  def genInc: Gen[Formula[Int]] = for {
    by <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    f <- Gen.lzy(genForm)
  } yield Inc(f, by)

  def genSum: Gen[Formula[Int]] = for {
    left <- Gen.lzy(genForm)
    // We have to make dags, so select from the closure of left sometimes
    right <- Gen.oneOf(genForm, Gen.oneOf(left.closure.toSeq))
  } yield Sum(left, right)
  def genProd: Gen[Formula[Int]] = for {
    left <- Gen.lzy(genForm)
    // We have to make dags, so select from the closure of left sometimes
    right <- Gen.oneOf(genForm, Gen.oneOf(left.closure.toSeq))
  } yield Product(left, right)

  type L[T] = Literal[T, Formula]

  /**
   * Here we convert our dag nodes into Literal[Formula, T]
   */
  def toLiteral = new GenFunction[Formula, L] {
    def apply[T] = { (form: Formula[T]) =>
      def recurse[T2](memo: HMap[Formula, L], f: Formula[T2]): (HMap[Formula, L], L[T2]) = memo.get(f) match {
        case Some(l) => (memo, l)
        case None => f match {
          case c @ Constant(_) =>
            def makeLit[T1](c: Constant[T1]) = {
              val lit: L[T1] = ConstLit(c)
              (memo + (c -> lit), lit)
            }
            makeLit(c)
          case inc @ Inc(_, _) =>
            def makeLit[T1](i: Inc[T1]) = {
              val (m1, f1) = recurse(memo, i.in)
              val lit = UnaryLit(f1, { f: Formula[T1] => Inc(f, i.by) })
              (m1 + (i -> lit), lit)
            }
            makeLit(inc)
          case sum @ Sum(_, _) =>
            def makeLit[T1](s: Sum[T1]) = {
              val (m1, fl) = recurse(memo, s.left)
              val (m2, fr) = recurse(m1, s.right)
              val lit = BinaryLit(fl, fr, { (f: Formula[T1], g: Formula[T1]) => Sum(f, g) })
              (m2 + (s -> lit), lit)
            }
            makeLit(sum)
          case prod @ Product(_, _) =>
            def makeLit[T1](p: Product[T1]) = {
              val (m1, fl) = recurse(memo, p.left)
              val (m2, fr) = recurse(m1, p.right)
              val lit = BinaryLit(fl, fr, { (f: Formula[T1], g: Formula[T1]) => Product(f, g) })
              (m2 + (p -> lit), lit)
            }
            makeLit(prod)
        }
      }
      recurse(HMap.empty[Formula, L], form)._2
    }
  }

  /**
   * Inc(Inc(a, b), c) = Inc(a, b + c)
   */
  object CombineInc extends Rule[Formula] {
    def apply[T](on: ExpressionDag[Formula]) = {
      case Inc(i @ Inc(a, b), c) if on.fanOut(i) == 1 => Some(Inc(a, b + c))
      case _ => None
    }
  }

  object RemoveInc extends PartialRule[Formula] {
    def applyWhere[T](on: ExpressionDag[Formula]) = {
      case Inc(f, by) => Sum(f, Constant(by))
    }
  }

  //Check the Node[T] <=> Id[T] is an Injection for all nodes reachable from the root

  property("toLiteral/Literal.evaluate is a bijection") = forAll(genForm) { form =>
    toLiteral.apply(form).evaluate == form
  }

  property("Going to ExpressionDag round trips") = forAll(genForm) { form =>
    val (dag, id) = ExpressionDag(form, toLiteral)
    dag.evaluate(id) == form
  }

  property("CombineInc does not change results") = forAll(genForm) { form =>
    val simplified = ExpressionDag.applyRule(form, toLiteral, CombineInc)
    form.evaluate == simplified.evaluate
  }

  property("RemoveInc removes all Inc") = forAll(genForm) { form =>
    val noIncForm = ExpressionDag.applyRule(form, toLiteral, RemoveInc)
    def noInc(f: Formula[Int]): Boolean = f match {
      case Constant(_) => true
      case Inc(_, _) => false
      case Sum(l, r) => noInc(l) && noInc(r)
      case Product(l, r) => noInc(l) && noInc(r)
    }
    noInc(noIncForm) && (noIncForm.evaluate == form.evaluate)
  }

  /**
   * This law is important for the rules to work as expected, and not have equivalent
   * nodes appearing more than once in the Dag
   */
  property("Node structural equality implies Id equality") = forAll(genForm) { form =>
    val (dag, id) = ExpressionDag(form, toLiteral)
    type BoolT[T] = Boolean // constant type function
    dag.idToExp.collect(new GenPartial[HMap[Id, ExpressionDag[Formula]#E]#Pair, BoolT] {
      def apply[T] = {
        case (id, expr) =>
          val node = expr.evaluate(dag.idToExp)
          dag.idOf(node) == id
      }
    }).forall(identity)
  }

  // The normal Inc gen recursively calls the general dag Generator
  def genChainInc: Gen[Formula[Int]] = for {
    by <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    chain <- genChain
  } yield Inc(chain, by)

  def genChain: Gen[Formula[Int]] = Gen.frequency((1, genConst), (3, genChainInc))
  property("CombineInc compresses linear Inc chains") = forAll(genChain) { chain =>
    ExpressionDag.applyRule(chain, toLiteral, CombineInc) match {
      case Constant(n) => true
      case Inc(Constant(n), b) => true
      case _ => false // All others should have been compressed
    }
  }

  /**
   * We should be able to totally evaluate these formulas
   */
  object EvaluationRule extends Rule[Formula] {
    def apply[T](on: ExpressionDag[Formula]) = {
      case Sum(Constant(a), Constant(b)) => Some(Constant(a + b))
      case Product(Constant(a), Constant(b)) => Some(Constant(a * b))
      case Inc(Constant(a), b) => Some(Constant(a + b))
      case _ => None
    }
  }
  property("EvaluationRule totally evaluates") = forAll(genForm) { form =>
    ExpressionDag.applyRule(form, toLiteral, EvaluationRule) match {
      case Constant(x) if x == form.evaluate => true
      case _ => false
    }
  }
}
