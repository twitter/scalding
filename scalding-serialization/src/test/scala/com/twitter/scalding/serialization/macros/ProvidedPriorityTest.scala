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

package com.twitter.scalding.serialization.macros
import scala.language.higherKinds
import java.io.{ ByteArrayOutputStream, InputStream }
import java.nio.ByteBuffer

import com.twitter.scalding.serialization.{
  JavaStreamEnrichments,
  Law,
  Law1,
  Law2,
  Law3,
  OrderedSerialization,
  Serialization
}
import org.scalatest.FunSuite
import com.twitter.scalding.serialization.StringOrderedSerialization
import com.twitter.scalding.serialization._

object CompanionOrderedSerialization {
  val v = (new StringOrderedSerialization).asInstanceOf[OrderedSerialization[SampleCaseClass]]
}
object ProvidedOrderedSerialization {
  val v = (new StringOrderedSerialization).asInstanceOf[OrderedSerialization[SampleCaseClass]]
}

object SampleCaseClass {
  implicit def ordSer: OrderedSerialization[SampleCaseClass] = CompanionOrderedSerialization.v
}
case class SampleCaseClass(v: Int)

object ProvidedImports {
  implicit def aProvided: Exported[OrderedSerialization[SampleCaseClass]] = Exported(ProvidedOrderedSerialization.v)
}
class ProvidedPriorityTest
  extends FunSuite {

  import ProvidedImports._
  def checkLocalOverride[T: OrderedSerialization] = {
    assert(implicitly[OrderedSerialization[T]] === CompanionOrderedSerialization.v)
  }

  test("Test that the imported scope doesn't override a companion object") {
    checkLocalOverride[SampleCaseClass]
  }

}
