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

import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.scalding.commons.thrift.TBaseOrderedBufferable
import org.apache.thrift.TBase

class MacrosUnitTests extends WordSpec with Matchers {
  def isMg[T](t: T): T = {
    t shouldBe a[MacroGenerated]
    t
  }

  private val dummy = new TBaseOrderedBufferable[Nothing] {
    override val minFieldId: Short = 1
    override val klass = classOf[Nothing]
  }

  def isMacroTBaseOrderedBufferableAvailable[T <: TBase[_, _]](implicit proof: TBaseOrderedBufferable[T] = dummy.asInstanceOf[TBaseOrderedBufferable[T]]) =
    proof.isInstanceOf[MacroGenerated]

  "MacroGenerated TBaseOrderedBufferable" should {
    "Generate the converter TestThriftStructure" in { Macros.toTBaseOrderedBufferable[com.twitter.scalding.commons.macros.TestThriftStructure] }

    "Generate a convertor for TestThriftStructure" in { isMacroTBaseOrderedBufferableAvailable[com.twitter.scalding.commons.macros.TestThriftStructure] shouldBe true }

  }
}
