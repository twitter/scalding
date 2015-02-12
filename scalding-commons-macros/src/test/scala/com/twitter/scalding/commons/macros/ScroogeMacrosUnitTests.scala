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
import com.twitter.scalding.commons.macros.impl.{ ScroogeInternalOrderedSerializationImpl, ScroogeTProtocolOrderedSerializationImpl }
import com.twitter.scalding.commons.macros.scalathrift._
import com.twitter.scalding.commons.thrift.ScroogeTProtocolOrderedSerialization
import com.twitter.scalding.macros._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scrooge.ThriftStruct
import org.scalatest.{ Matchers, WordSpec }
import scala.language.experimental.macros
import org.scalacheck.Arbitrary
import org.scalatest.prop.PropertyChecks

class ScroogeMacrosUnitTests extends WordSpec with Matchers with PropertyChecks {
  import TestHelper._
  import ScroogeGenerators._

  private val dummy = new ScroogeTProtocolOrderedSerialization[Nothing] {
    override val minFieldId: Short = 1
    override val thriftStructSerializer = null
  }

  def isMacroScroogeOrderedSerializationAvailable[T <: ThriftStruct](implicit proof: ScroogeTProtocolOrderedSerialization[T] = dummy.asInstanceOf[ScroogeTProtocolOrderedSerialization[T]]) =
    proof.isInstanceOf[MacroGenerated]
  import com.twitter.scalding.commons.macros.Macros._

  "MacroGenerated TBaseOrderedSerialization" should {
    "Generate the converter TestThriftStructure" in { Macros.toScroogeTProtocolOrderedSerialization[TestLists] }

    "Should RT" in {
      forAll { a1: TestLists =>
        assert(oBufCompare(rt(a1), a1) == 0)
      }
    }

    "Should Compare Equal" in {
      val x1 = ScroogeGenerators.dataProvider[TestLists](1)
      val x2 = ScroogeGenerators.dataProvider[TestLists](1)
      compareSerialized(x1, x2) shouldEqual OrderedSerialization.Equal
      compareSerialized(x1, x2)(Macros.toScroogeInternalOrderedSerialization[TestLists]) shouldEqual OrderedSerialization.Equal
    }

    "Should Compare Not Equal" in {
      val x1 = ScroogeGenerators.dataProvider[TestLists](1)
      val x2 = ScroogeGenerators.dataProvider[TestLists](2)
      assert(compareSerialized(x1, x2) != OrderedSerialization.Equal)
    }

    "Should RT correctly" in {
      class Container[T](implicit oSer: OrderedSerialization[T]) {
        def ord: OrderedSerialization[(Long, T)] = {
          implicitly[OrderedSerialization[(Long, T)]]
        }
      }

      val ordSer = (new Container[TestLists]).ord

      forAll { a1: (Long, TestLists) =>
        assert(oBufCompare(rt(a1)(ordSer), a1)(ordSer) == 0)
      }
    }
  }
}
