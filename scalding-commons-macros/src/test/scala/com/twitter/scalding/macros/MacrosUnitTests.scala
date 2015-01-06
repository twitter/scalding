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

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.macros._
import com.twitter.scalding.commons.macros._
import com.twitter.scalding.serialization.Externalizer
import com.twitter.scalding.typed.OrderedBufferable
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.scalding.commons.thrift.TBaseOrderedBufferable
import org.apache.thrift.TBase
import java.nio.ByteBuffer
import scala.util.Success

class MacrosUnitTests extends WordSpec with Matchers {
  def isMg[T](t: T): T = {
    t shouldBe a[MacroGenerated]
    t
  }

  private val dummy = new TBaseOrderedBufferable[Nothing] {
    override val minFieldId: Short = 1
    @transient lazy val prototype: Nothing = null.asInstanceOf[Nothing]
  }

  def rt[T <: TBase[_, _]](t: T)(implicit orderedBuffer: TBaseOrderedBufferable[T]) {
    val buf = serialize[T](t)
    assert(orderedBuffer.get(buf).map(_._2) === Success(t))
  }

  def serialize[T <: TBase[_, _]](t: T)(implicit orderedBuffer: TBaseOrderedBufferable[T]): ByteBuffer = {
    val buf = ByteBuffer.allocate(50)
    orderedBuffer.put(buf, t)
    buf.position(0)
    buf
  }
  def compareSerialized[T <: TBase[_, _]](a: T, b: T)(implicit orderedBuffer: TBaseOrderedBufferable[T]): OrderedBufferable.Result = {
    val bufA = serialize[T](a)
    val bufB = serialize[T](b)
    orderedBuffer.compareBinary(bufA, bufB)
  }

  def isMacroTBaseOrderedBufferableAvailable[T <: TBase[_, _]](implicit proof: TBaseOrderedBufferable[T] = dummy.asInstanceOf[TBaseOrderedBufferable[T]]) =
    proof.isInstanceOf[MacroGenerated]

  "MacroGenerated TBaseOrderedBufferable" should {
    "Generate the converter TestThriftStructure" in { Macros.toTBaseOrderedBufferable[com.twitter.scalding.commons.macros.TestThriftStructure] }

    "Should RT" in {
      val x = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      rt[com.twitter.scalding.commons.macros.TestThriftStructure](x)
    }

    "Should Compare Equal" in {
      val x1 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      val x2 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      compareSerialized(x1, x2) shouldEqual OrderedBufferable.Equal
    }

    "Should Compare LessThan" in {
      val x1 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 122)
      val x2 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      compareSerialized(x1, x2) shouldEqual OrderedBufferable.Less
    }

    "Should Compare GreaterThan" in {
      val x1 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdp", 123)
      val x2 = new com.twitter.scalding.commons.macros.TestThriftStructure("asdf", 123)
      compareSerialized(x1, x2) shouldEqual OrderedBufferable.Greater
    }

  }
}