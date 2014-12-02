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
package com.twitter.scalding.typed

import org.scalacheck.{ Arbitrary, Gen, Properties }
import org.scalacheck.Gen._
import org.scalacheck.Prop.forAll

import com.twitter.algebird.{ Monoid, BaseProperties }

object TakeSeq {
  sealed trait Box[+T] {
    def size: Int
    def take(n: Int): Box[T]
    def ++[U >: T](that: Box[U]): Box[U] = that match {
      case Empty => this
      case _ => Concat(this, that)
    }
    def iterator: Iterator[T]
  }
  case object Empty extends Box[Nothing] {
    def size = 0
    def take(n: Int) = Empty
    def iterator = Iterator.empty
    override def ++[U >: Nothing](that: Box[U]): Box[U] = that
  }
  case class Item[+T](get: T) extends Box[T] {
    def size = 1
    def take(n: Int) =
      if (n < 1) Empty
      else this
    def iterator = Iterator(get)
  }
  case class Concat[T](first: Box[T], rest: Box[T]) extends Box[T] {
    val size = first.size + rest.size
    def take(n: Int) =
      if (n < 1) Empty
      else if (n >= size) this
      else {
        val restTake = n - first.size
        if (restTake <= 0) first.take(n)
        else Concat(first, rest.take(restTake))
      }
    // TODO: This is almost certainly unacceptably slow
    // to get good speed we probably need to make a stack and walk the tree
    def iterator = first.iterator ++ rest.iterator
  }

  def monoid[T](count: Int): Monoid[Box[T]] = new Monoid[Box[T]] {
    def zero = Empty
    def plus(left: Box[T], right: Box[T]) = Concat(left, right).take(count)
    override def sumOption(to: TraversableOnce[Box[T]]): Option[Box[T]] =
      if (to.isEmpty) None
      else {
        var res: Box[T] = Empty
        var iter = to.toIterator
        while ((res.size < count) && iter.hasNext) {
          res = Concat(res, iter.next)
        }
        Some(res.take(count))
      }
  }
}

object TakeMonoidTest extends Properties("TakeMonoidTest") {
  import BaseProperties.monoidLawsEq
  import TakeSeq._

  def eq[T](left: Box[T], right: Box[T]): Boolean =
    (left.size == right.size) && (left.iterator.toList == right.iterator.toList)

  val TestSize = 10

  implicit val monoid = TakeSeq.monoid[Int](TestSize)

  implicit def arb[T: Arbitrary]: Arbitrary[Box[T]] = {
    val gen = Arbitrary.arbitrary[T]

    def genBox: Gen[Box[T]] =
      Gen.frequency((1, TakeSeq.Empty),
        (2, gen.map(TakeSeq.Item(_))),
        (2, Gen.lzy(for { a <- genBox; b <- genBox } yield TakeSeq.Concat(a, b))))

    Arbitrary(genBox.map(_.take(TestSize))) //if you generate too large, the zero law fails
  }

  property("Take is a Monoid") = monoidLawsEq[Box[Int]](eq)
}
