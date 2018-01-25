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
import org.scalacheck.{ Arbitrary, Gen, Properties }

object LiteralTests extends Properties("Literal") {
  case class Box[T](get: T)

  def transitiveClosure[N[_]](l: Literal[_, N], acc: Set[Literal[_, N]] = Set.empty[Literal[_, N]]): Set[Literal[_, N]] = l match {
    case c @ ConstLit(_) => acc + c
    case u @ UnaryLit(prev, _) => if (acc(u)) acc else transitiveClosure(prev, acc + u)
    case b @ BinaryLit(p1, p2, _) => if (acc(b)) acc else transitiveClosure(p2, transitiveClosure(p1, acc + b))
  }

  def genBox: Gen[Box[Int]] = Gen.chooseNum(Int.MinValue, Int.MaxValue).map(Box(_))

  def genConst: Gen[Literal[Int, Box]] = genBox.map(ConstLit(_))
  def genUnary: Gen[Literal[Int, Box]] = for {
    fn <- Arbitrary.arbitrary[(Int) => (Int)]
    bfn = { case Box(b) => Box(fn(b)) }: Box[Int] => Box[Int]
    input <- genLiteral
  } yield UnaryLit(input, bfn)

  def genBinary: Gen[Literal[Int, Box]] = for {
    fn <- Arbitrary.arbitrary[(Int, Int) => (Int)]
    bfn = { case (Box(l), Box(r)) => Box(fn(l, r)) }: (Box[Int], Box[Int]) => Box[Int]
    left <- genLiteral
    // We have to make dags, so select from the closure of left sometimes
    right <- Gen.oneOf(genLiteral, genChooseFrom(transitiveClosure[Box](left)))
  } yield BinaryLit(left, right, bfn)

  def genChooseFrom[N[_]](s: Set[Literal[_, N]]): Gen[Literal[Int, N]] =
    Gen.oneOf(s.toSeq.asInstanceOf[Seq[Literal[Int, N]]])

  /*
   * Create dags. Don't use binary too much as it can create exponentially growing dags
   */
  def genLiteral: Gen[Literal[Int, Box]] = Gen.frequency((3, genConst),
    (6, genUnary), (1, genBinary))

  //This evaluates by recursively walking the tree without memoization
  //as lit.evaluate should do
  def slowEvaluate[T](lit: Literal[T, Box]): Box[T] = lit match {
    case ConstLit(n) => n
    case UnaryLit(in, fn) => fn(slowEvaluate(in))
    case BinaryLit(a, b, fn) => fn(slowEvaluate(a), slowEvaluate(b))
  }

  property("Literal.evaluate must match simple explanation") = forAll(genLiteral) { (l: Literal[Int, Box]) =>
    l.evaluate == slowEvaluate(l)
  }
}
