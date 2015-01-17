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
package com.twitter.scalding.commons.macros

import com.twitter.bijection.Bufferable
import com.twitter.bijection.macros.MacroGenerated
import com.twitter.scalding._
import com.twitter.scalding.commons.macros._
import com.twitter.scalding.commons.macros.impl.TBaseOrderedSerializationImpl
import com.twitter.scalding.commons.thrift.TBaseOrderedSerialization
import com.twitter.scalding.macros._
import com.twitter.scalding.serialization.OrderedSerialization
import org.apache.thrift.TBase
import org.scalatest.{ Matchers, WordSpec }
import scala.language.experimental.macros

class TBaseMacrosUnitTests extends WordSpec with Matchers {
  import TestHelper._
  implicit def toTBaseOrderedSerialization[T <: TBase[_, _]]: TBaseOrderedSerialization[T] = macro TBaseOrderedSerializationImpl[T]

  private val dummy = new TBaseOrderedSerialization[Nothing] {
    override val minFieldId: Short = 1
    override def compare(a: Nothing, b: Nothing) = 0
    @transient lazy val prototype: Nothing = null.asInstanceOf[Nothing]
  }

  def isMacroTBaseOrderedSerializationAvailable[T <: TBase[_, _]](implicit proof: TBaseOrderedSerialization[T] = dummy.asInstanceOf[TBaseOrderedSerialization[T]]) =
    proof.isInstanceOf[MacroGenerated]

  "MacroGenerated TBaseOrderedSerialization" should {
    "Generate the converter TestThriftStructure" in { Macros.toTBaseOrderedSerialization[com.twitter.scalding.commons.macros.TestThriftStructure] }

    "Should RT" in {
      val x = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      rt[com.twitter.scalding.commons.macros.TestThriftStructure](x)
    }

    "Should Compare Equal" in {
      val x1 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      val x2 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      implicitly[OrderedSerialization[com.twitter.scalding.commons.macros.TestThriftStructure]].compare(x1, x2)
      rawCompare(x1, x2) shouldEqual 0
    }

    "Should Compare LessThan" in {
      val x1 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 122)
      val x2 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      rawCompare(x1, x2) shouldEqual -1
    }

    "Should Compare GreaterThan" in {
      val x1 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdp", 123)
      val x2 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      rawCompare(x1, x2) shouldEqual 1
    }

  }
}
