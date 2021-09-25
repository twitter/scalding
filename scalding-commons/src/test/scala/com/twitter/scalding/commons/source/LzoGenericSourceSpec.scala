/*
Copyright 2015 Twitter, Inc.

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
package com.twitter.scalding.commons.source

import com.twitter.bijection.JavaSerializationInjection
import org.scalatest.{ Matchers, WordSpec }
import scala.util.Success

class LzoGenericSourceSpec extends WordSpec with Matchers {
  "LzoGenericScheme" should {
    "be serializable" in {
      val scheme = LzoGenericScheme[Array[Byte]](IdentityBinaryConverter)
      val inj = JavaSerializationInjection[LzoGenericScheme[Array[Byte]]]
      inj.invert(inj.apply(scheme)) shouldBe Success(scheme)
    }
  }
}
