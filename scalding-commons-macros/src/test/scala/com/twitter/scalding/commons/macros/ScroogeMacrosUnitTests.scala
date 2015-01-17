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

import com.twitter.bijection.macros.MacroGenerated
import com.twitter.scalding._
import com.twitter.scalding.commons.macros._
import com.twitter.scalding.commons.macros.impl.{ ScroogeInternalOrderedSerializationImpl, ScroogeOrderedSerializationImpl }
import com.twitter.scalding.commons.macros.scalathrift._
import com.twitter.scalding.commons.thrift.ScroogeOrderedSerialization
import com.twitter.scalding.macros._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scrooge.ThriftStruct
import org.scalatest.{ Matchers, WordSpec }
import scala.language.experimental.macros

class ScroogeMacrosUnitTests extends WordSpec with Matchers {
  import TestHelper._
  import ScroogeGenerators._

  private val dummy = new ScroogeOrderedSerialization[Nothing] {
    override val minFieldId: Short = 1
    override val thriftStructSerializer = null
  }

  def isMacroScroogeOrderedSerializationAvailable[T <: ThriftStruct](implicit proof: ScroogeOrderedSerialization[T] = dummy.asInstanceOf[ScroogeOrderedSerialization[T]]) =
    proof.isInstanceOf[MacroGenerated]

  "MacroGenerated TBaseOrderedSerialization" should {
    "Generate the converter TestThriftStructure" in { Macros.toScroogeOrderedSerialization[TestLists] }

    "Should RT" in {
      implicit def toScroogeInternalOrderedSerialization[T]: OrderedSerialization[T] = macro ScroogeInternalOrderedSerializationImpl[T]
      val x: TestLists = ScroogeGenerators.dataProvider[TestLists](1)
      assert(oBufCompare(rt(x), x) == 0)
    }

    "Should Compare Equal" in {
      implicit def toScroogeInternalOrderedSerialization[T]: OrderedSerialization[T] = macro ScroogeInternalOrderedSerializationImpl[T]
      val x1 = ScroogeGenerators.dataProvider[TestLists](1)
      val x2 = ScroogeGenerators.dataProvider[TestLists](1)
      compareSerialized(x1, x2) shouldEqual OrderedSerialization.Equal
      compareSerialized(x1, x2)(Macros.toScroogeInternalOrderedSerialization[TestLists]) shouldEqual OrderedSerialization.Equal
    }

    "Should Compare Not Equal" in {
      implicit def toScroogeInternalOrderedSerialization[T]: OrderedSerialization[T] = macro ScroogeInternalOrderedSerializationImpl[T]
      val x1 = ScroogeGenerators.dataProvider[TestLists](1)
      val x2 = ScroogeGenerators.dataProvider[TestLists](2)
      assert(compareSerialized(x1, x2) != OrderedSerialization.Equal)
    }

  }
}
