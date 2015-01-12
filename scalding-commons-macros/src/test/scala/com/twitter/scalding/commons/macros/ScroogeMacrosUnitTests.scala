// /*
//  Copyright 2014 Twitter, Inc.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at

//  http://www.apache.org/licenses/LICENSE-2.0

//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  */
// package com.twitter.scalding.commons.macros

// import org.scalatest.{ Matchers, WordSpec }

// import com.twitter.scalding._
// import com.twitter.scalding.macros._
// import com.twitter.scalding.commons.macros._
// import com.twitter.scalding.serialization.Externalizer
// import com.twitter.scalding.typed.OrderedBufferable
// import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
// import com.twitter.scalding.commons.thrift.ScroogeOrderedBufferable
// import com.twitter.scalding.commons.macros.impl.ScroogeOrderedBufferableImpl
// import scala.language.experimental.macros
// import com.twitter.scrooge.ThriftStruct
// import com.twitter.scalding.commons.macros.scalathrift.{ TestLists, TestStruct }
// import java.nio.ByteBuffer
// import scala.util.Success
// import com.twitter.bijection.Bufferable

// class ScroogeMacrosUnitTests extends WordSpec with Matchers {
//   import ScroogeGenerators._

//   implicit def toScroogerderedBufferable[T <: ThriftStruct]: ScroogeOrderedBufferable[T] = macro ScroogeOrderedBufferableImpl[T]

//   def isMg[T](t: T): T = {
//     t shouldBe a[MacroGenerated]
//     t
//   }

//   private val dummy = new ScroogeOrderedBufferable[Nothing] {
//     override val minFieldId: Short = 1
//     override val thriftStructSerializer = null
//   }

//   def rt[T](t: T)(implicit orderedBuffer: OrderedBufferable[T]) = {
//     val buf = serialize[T](t)
//     orderedBuffer.get(buf).map(_._2).get
//   }

//   def oBufCompare[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]): Int = {
//     obuf.compare(a, b)
//   }

//   def serialize[T](t: T)(implicit orderedBuffer: OrderedBufferable[T]): ByteBuffer = {
//     val buf = ByteBuffer.allocate(128)
//     Bufferable.reallocatingPut(buf) { bb =>
//       orderedBuffer.put(bb, t)
//       bb.position(0)
//       bb
//     }
//   }

//   def compareSerialized[T <: ThriftStruct](a: T, b: T)(implicit orderedBuffer: OrderedBufferable[T]): OrderedBufferable.Result = {
//     val bufA = serialize[T](a)
//     val bufB = serialize[T](b)
//     orderedBuffer.compareBinary(bufA, bufB)
//   }

//   def isMacroScroogeOrderedBufferableAvailable[T <: ThriftStruct](implicit proof: ScroogeOrderedBufferable[T] = dummy.asInstanceOf[ScroogeOrderedBufferable[T]]) =
//     proof.isInstanceOf[MacroGenerated]

//   "MacroGenerated TBaseOrderedBufferable" should {
//     "Generate the converter TestThriftStructure" in { Macros.toScroogeOrderedBufferable[TestLists] }

//     "Should RT" in {
//       val x: TestLists = ScroogeGenerators.dataProvider[TestLists](1)
//       assert(oBufCompare(rt(x), x) == 0)
//     }

//     "Should Compare Equal" in {
//       val x1 = ScroogeGenerators.dataProvider[TestLists](1)
//       val x2 = ScroogeGenerators.dataProvider[TestLists](1)
//       compareSerialized(x1, x2) shouldEqual OrderedBufferable.Equal
//       compareSerialized(x1, x2)(Macros.toScroogeInternalOrderedBufferable[TestLists]) shouldEqual OrderedBufferable.Equal
//     }

//     "Should Compare Not Equal" in {

//       val x1 = ScroogeGenerators.dataProvider[TestLists](1)
//       val x2 = ScroogeGenerators.dataProvider[TestLists](2)
//       assert(compareSerialized(x1, x2) != OrderedBufferable.Equal)
//     }

//   }
// }
