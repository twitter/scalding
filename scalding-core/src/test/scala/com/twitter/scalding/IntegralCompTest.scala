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

import org.scalatest.{ Matchers, WordSpec }

class IntegralCompTest extends WordSpec with Matchers {
  def box[T](t: T) = t.asInstanceOf[AnyRef]

  "IntegralComparator" should {
    val intComp = new IntegralComparator
    "recognize integral types" in {
      intComp.isIntegral(box(1)) shouldBe true
      intComp.isIntegral(box(1L)) shouldBe true
      intComp.isIntegral(box(1: Short)) shouldBe true
      //Boxed
      intComp.isIntegral(new java.lang.Long(2)) shouldBe true
      intComp.isIntegral(new java.lang.Integer(2)) shouldBe true
      intComp.isIntegral(new java.lang.Short(2: Short)) shouldBe true
      //These are not integrals
      intComp.isIntegral(box(0.0)) shouldBe false
      intComp.isIntegral(box("hey")) shouldBe false
      intComp.isIntegral(box(Nil)) shouldBe false
      intComp.isIntegral(box(None)) shouldBe false
    }
    "handle null inputs" in {
      intComp.hashCode(null) shouldBe 0
      List(box(1), box("hey"), box(2L), box(0.0)).foreach { x =>
        intComp.compare(null, x) should be < (0)
        intComp.compare(x, null) should be > (0)
        intComp.compare(x, x) shouldBe 0
      }
      intComp.compare(null, null) shouldBe 0
    }
    "have consistent hashcode" in {
      List((box(1), box(1L)), (box(2), box(2L)), (box(3), box(3L)))
        .foreach { pair =>
          intComp.compare(pair._1, pair._2) shouldBe 0
          intComp.hashCode(pair._1) shouldBe (intComp.hashCode(pair._2))
        }
      List((box(1), box(2L)), (box(2), box(3L)), (box(3), box(4L)))
        .foreach { pair =>
          intComp.compare(pair._1, pair._2) should be < (0)
          intComp.compare(pair._2, pair._1) should be > (0)
        }
    }
    "Compare strings properly" in {
      intComp.compare("hey", "you") shouldBe ("hey".compareTo("you"))
      intComp.compare("hey", "hey") shouldBe ("hey".compareTo("hey"))
      intComp.compare("you", "hey") shouldBe ("you".compareTo("hey"))
    }
  }
}
