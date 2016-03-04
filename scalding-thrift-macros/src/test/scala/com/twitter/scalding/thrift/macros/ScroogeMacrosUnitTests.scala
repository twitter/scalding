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
package com.twitter.scalding.thrift.macros

import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.thrift.macros.scalathrift._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ Matchers, WordSpec }

import scala.language.experimental.macros

class ScroogeMacrosUnitTests extends WordSpec with Matchers with PropertyChecks {
  import ScroogeGenerators._
  import TestHelper._
  import Macros._

  "MacroGenerated TBaseOrderedSerialization" should {

    "Should generate serializers" in {
      Macros.scroogeOrdSer[TestTypes]
      Macros.scroogeOrdSer[TestLists]
      Macros.scroogeOrdSer[TestMaps]
      Macros.scroogeOrdSer[TestOptionTypes]
      Macros.scroogeOrdSer[A]
    }

    "Should RT" in {
      forAll { a1: TestLists =>
        assert(oBufCompare(rt(a1), a1) == 0)
      }
    }

    "Should Compare Equal" in {
      val x1 = ScroogeGenerators.dataProvider[TestLists](1)
      val x2 = ScroogeGenerators.dataProvider[TestLists](1)
      compareSerialized(x1, x2) shouldEqual OrderedSerialization.Equal
      compareSerialized(x1, x2)(Macros.scroogeOrdSer[TestLists]) shouldEqual OrderedSerialization.Equal
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
