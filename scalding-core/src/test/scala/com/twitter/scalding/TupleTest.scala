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
package com.twitter.scalding

import cascading.tuple.{ TupleEntry, Tuple => CTuple }

import org.scalatest.{ Matchers, WordSpec }

class TupleTest extends WordSpec with Matchers {
  def get[T](ctup: CTuple)(implicit tc: TupleConverter[T]) = tc(new TupleEntry(ctup))
  def set[T](t: T)(implicit ts: TupleSetter[T]): CTuple = ts(t)

  def arityConvMatches[T](t: T, ar: Int)(implicit tc: TupleConverter[T]): Boolean = {
    assert(t != null)
    tc.arity == ar
  }
  def aritySetMatches[T](t: T, ar: Int)(implicit tc: TupleSetter[T]): Boolean = {
    assert(t != null)
    tc.arity == ar
  }

  def roundTrip[T](t: T)(implicit tc: TupleConverter[T], ts: TupleSetter[T]): Boolean =
    tc(new TupleEntry(ts(t))) == t

  "TupleConverters" should {

    "TupleGetter should work as a type-class" in {
      val emptyTup = new CTuple
      val ctup = new CTuple("hey", new java.lang.Long(2), new java.lang.Integer(3), emptyTup)
      TupleGetter.get[String](ctup, 0) shouldBe "hey"
      TupleGetter.get[Long](ctup, 1) shouldBe 2L
      TupleGetter.get[Int](ctup, 2) shouldBe 3
      TupleGetter.get[CTuple](ctup, 3) shouldBe emptyTup
    }

    "get primitives out of cascading tuples" in {
      val ctup = new CTuple("hey", new java.lang.Long(2), new java.lang.Integer(3))
      get[(String, Long, Int)](ctup) shouldBe ("hey", 2L, 3)

      roundTrip[Int](3) shouldBe true
      arityConvMatches(3, 1) shouldBe true
      aritySetMatches(3, 1) shouldBe true
      roundTrip[Long](42L) shouldBe true
      arityConvMatches(42L, 1) shouldBe true
      aritySetMatches(42L, 1) shouldBe true
      roundTrip[String]("hey") shouldBe true
      arityConvMatches("hey", 1) shouldBe true
      aritySetMatches("hey", 1) shouldBe true
      roundTrip[(Int, Int)]((4, 2)) shouldBe true
      arityConvMatches((2, 3), 2) shouldBe true
      aritySetMatches((2, 3), 2) shouldBe true
    }
    "get non-primitives out of cascading tuples" in {
      val ctup = new CTuple(None, List(1, 2, 3), 1 -> 2)
      get[(Option[Int], List[Int], (Int, Int))](ctup) shouldBe (None, List(1, 2, 3), 1 -> 2)

      roundTrip[(Option[Int], List[Int])]((Some(1), List())) shouldBe true
      arityConvMatches((None, Nil), 2) shouldBe true
      aritySetMatches((None, Nil), 2) shouldBe true

      arityConvMatches(None, 1) shouldBe true
      aritySetMatches(None, 1) shouldBe true
      arityConvMatches(List(1, 2, 3), 1) shouldBe true
      aritySetMatches(List(1, 2, 3), 1) shouldBe true
    }
    "deal with AnyRef" in {
      val ctup = new CTuple(None, List(1, 2, 3), 1 -> 2)
      get[(AnyRef, AnyRef, AnyRef)](ctup) shouldBe (None, List(1, 2, 3), 1 -> 2)
      get[AnyRef](new CTuple("you")) shouldBe "you"

      roundTrip[AnyRef]("hey") shouldBe true
      roundTrip[(AnyRef, AnyRef)]((Nil, Nil)) shouldBe true
      arityConvMatches[(AnyRef, AnyRef)](("hey", "you"), 2) shouldBe true
      aritySetMatches[(AnyRef, AnyRef)](("hey", "you"), 2) shouldBe true
    }
  }
}
