/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.scalding.macros

import org.scalatest.{ FunSuite, ShouldMatchers }
import org.scalatest.prop.PropertyChecks
import scala.language.experimental.macros
import com.twitter.scalding.typed.OrderedBufferable
import java.nio.ByteBuffer
import org.scalacheck.Arbitrary
trait LowerPriorityImplicit {
  implicit def primitiveOrderedBufferSupplier[T] = macro com.twitter.scalding.macros.impl.OrderedBufferableProviderImpl[T]
}
class MacroOrderingProperties extends FunSuite with PropertyChecks with ShouldMatchers with LowerPriorityImplicit {

  def serialize[T](t: T)(implicit orderedBuffer: OrderedBufferable[T]): ByteBuffer = {
    val buf = ByteBuffer.allocate(128)
    orderedBuffer.put(buf, t)
    buf.position(0)
    buf
  }

  def rawCompare[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]): Int = {
    obuf.compareBinary(serialize(a), serialize(b)).unsafeToInt
  }

  def oBufCompare[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]): Int = {
    obuf.compare(a, b)
  }

  def clamp(i: Int): Int =
    i match {
      case x if x < 0 => -1
      case x if x > 0 => 1
      case x => 0
    }
  def check[T: Arbitrary](implicit ord: Ordering[T], obuf: OrderedBufferable[T]) = forAll { (a: T, b: T) =>
    rawCompare(a, b) + rawCompare(b, a) shouldEqual 0
    oBufCompare(a, b) + oBufCompare(b, a) shouldEqual 0
    assert(rawCompare(a, b) === clamp(ord.compare(a, b)))
    assert(oBufCompare(a, b) === ord.compare(a, b))
  }

  test("Test out Int") {
    implicit val localOrdering = Ordering.ordered[Int](identity)
    check[Int]
  }

  test("Test out Long") {
    implicit val localOrdering = Ordering.ordered[Long](identity)
    check[Long]
  }

  test("Test out Short") {
    implicit val localOrdering = Ordering.ordered[Short](identity)
    check[Short]
  }

  test("Test out Float") {
    implicit val localOrdering = Ordering.ordered[Float](identity)
    check[Float]
  }

  test("Test out Double") {
    implicit val localOrdering = Ordering.ordered[Double](identity)
    check[Double]
  }
}