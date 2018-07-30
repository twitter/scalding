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
package com.twitter.scalding.macros

import org.scalatest.WordSpec

/**
 * This test is intended to ensure that the macros do not require any imported code in scope. This is why all
 * references are via absolute paths.
 */
class MacroDepHygiene extends WordSpec {

  case class A(x: Int, y: String)
  case class B(x: A, y: String, z: A)
  class C

  def isMg(a: Any) = a.isInstanceOf[com.twitter.bijection.macros.MacroGenerated]

  "TupleSetter macro" should {
    def isTupleSetterAvailable[T](implicit proof: com.twitter.scalding.TupleSetter[T]) = isMg(proof)

    "work fine without any imports" in {
      com.twitter.scalding.macros.Macros.caseClassTupleSetter[A]
      com.twitter.scalding.macros.Macros.caseClassTupleSetter[B]
    }

    "implicitly work fine without any imports" in {
      import com.twitter.scalding.macros.MacroImplicits.materializeCaseClassTupleSetter
      assert(isTupleSetterAvailable[A])
      assert(isTupleSetterAvailable[B])
    }

    "fail if not a case class" in {
      assert(!isTupleSetterAvailable[C])
    }
  }

  "TupleConverter macro" should {
    def isTupleConverterAvailable[T](implicit proof: com.twitter.scalding.TupleConverter[T]) = isMg(proof)

    "work fine without any imports" in {
      com.twitter.scalding.macros.Macros.caseClassTupleConverter[A]
      com.twitter.scalding.macros.Macros.caseClassTupleConverter[B]
    }

    "implicitly work fine without any imports" in {
      import com.twitter.scalding.macros.MacroImplicits.materializeCaseClassTupleConverter
      assert(isTupleConverterAvailable[A])
      assert(isTupleConverterAvailable[B])
    }

    "fail if not a case class" in {
      assert(!isTupleConverterAvailable[C])
    }
  }
}
