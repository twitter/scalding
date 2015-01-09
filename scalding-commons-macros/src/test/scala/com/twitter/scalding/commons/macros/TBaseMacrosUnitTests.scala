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
import com.twitter.scalding.commons.macros.impl.TBaseOrderedBufferableImpl
import scala.language.experimental.macros
import org.apache.thrift.TBase
import java.nio.ByteBuffer
import scala.util.Success
import com.twitter.bijection.Bufferable

class TBaseMacrosUnitTests extends WordSpec with Matchers {
  implicit def toTBaseOrderedBufferable[T <: TBase[_, _]]: TBaseOrderedBufferable[T] = macro TBaseOrderedBufferableImpl[T]

  def isMg[T](t: T): T = {
    t shouldBe a[MacroGenerated]
    t
  }

  private val dummy = new TBaseOrderedBufferable[Nothing] {
    override val minFieldId: Short = 1
    override def compare(a: Nothing, b: Nothing) = 0
    @transient lazy val prototype: Nothing = null.asInstanceOf[Nothing]
  }

  def rt[T <: TBase[_, _]](t: T)(implicit orderedBuffer: OrderedBufferable[T]) {
    val buf = serialize[T](t)
    assert(orderedBuffer.get(buf).map(_._2) === Success(t))
  }

  def serialize[T <: TBase[_, _]](t: T)(implicit orderedBuffer: OrderedBufferable[T]): ByteBuffer = {
    val buf = ByteBuffer.allocate(128)
    Bufferable.reallocatingPut(buf) { bb =>
      orderedBuffer.put(bb, t)
      bb.position(0)
      bb
    }
  }
  def compareSerialized[T <: TBase[_, _]](a: T, b: T)(implicit orderedBuffer: OrderedBufferable[T]): OrderedBufferable.Result = {
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
      implicitly[OrderedBufferable[com.twitter.scalding.commons.macros.TestThriftStructure]].compare(x1, x2)
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
