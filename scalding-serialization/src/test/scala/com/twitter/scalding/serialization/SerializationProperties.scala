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
package com.twitter.scalding.serialization

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import org.scalacheck.Prop._

import JavaStreamEnrichments._
import java.io._
import scala.util.{ Try, Success }

object LawTester {
  def apply[T: Arbitrary](base: String, laws: Iterable[Law[T]]): Properties =
    new LawTester(implicitly[Arbitrary[T]].arbitrary, base, laws) {}
}

abstract class LawTester[T](g: Gen[T], base: String, laws: Iterable[Law[T]]) extends Properties(base) {
  laws.foreach {
    case Law1(name, fn) => property(name) = forAll(g)(fn)
    case Law2(name, fn) => property(name) = forAll(g, g)(fn)
    case Law3(name, fn) => property(name) = forAll(g, g, g)(fn)
  }
}

object SerializationProperties extends Properties("SerializationProperties") {

  import OrderedSerialization.readThenCompare

  implicit val intOrderedSerialization: OrderedSerialization[Int] = new OrderedSerialization[Int] {
    def read(in: InputStream) = Try(Reader.read[Int](in))
    def write(o: OutputStream, t: Int) = Try(Writer.write[Int](o, t))
    def hash(t: Int) = t.hashCode
    def compare(a: Int, b: Int) = java.lang.Integer.compare(a, b)
    def compareBinary(a: InputStream, b: InputStream) =
      readThenCompare(a, b)(this)
    val staticSize = Some(4)
    def dynamicSize(i: Int) = staticSize
  }

  implicit val stringOrdSer: OrderedSerialization[String] = new StringOrderedSerialization

  class IntWrapperClass(val x: Int)

  implicit val myIntWrapperOrdSer: OrderedSerialization[IntWrapperClass] =
    OrderedSerialization.viaTransform[IntWrapperClass, Int](_.x, new IntWrapperClass(_))

  class IntTryWrapperClass(val x: Int)

  implicit val myTryIntWrapperOrdSer: OrderedSerialization[IntTryWrapperClass] =
    OrderedSerialization.viaTryTransform[IntTryWrapperClass, Int](_.x, { x: Int => Success(new IntTryWrapperClass(x)) })

  implicit val arbIntWrapperClass: Arbitrary[IntWrapperClass] =
    Arbitrary(implicitly[Arbitrary[Int]].arbitrary.map(new IntWrapperClass(_)))

  implicit val arbIntTryWrapperClass: Arbitrary[IntTryWrapperClass] =
    Arbitrary(implicitly[Arbitrary[Int]].arbitrary.map(new IntTryWrapperClass(_)))

  implicit def tuple[A: OrderedSerialization, B: OrderedSerialization]: OrderedSerialization[(A, B)] =
    new OrderedSerialization2[A, B](implicitly, implicitly)

  def serializeSequenceCompare[T: OrderedSerialization](g: Gen[T]): Prop = forAll(Gen.listOf(g)) { list =>
    // make sure the list is even in size:
    val pairList = (if (list.size % 2 == 1) list.tail else list).grouped(2)
    val baos1 = new ByteArrayOutputStream
    val baos2 = new ByteArrayOutputStream
    pairList.foreach {
      case Seq(a, b) =>
        Serialization.write(baos1, a)
        Serialization.write(baos2, b)
      case _ => sys.error("unreachable")
    }
    // now the compares must match:
    val in1 = baos1.toInputStream
    val in2 = baos2.toInputStream
    pairList.forall {
      case Seq(a, b) =>
        OrderedSerialization.compareBinary[T](in1, in2) ==
          OrderedSerialization.resultFrom(OrderedSerialization.compare(a, b))
      case _ => sys.error("unreachable")
    }
  }

  def serializeSequenceCompare[T: OrderedSerialization: Arbitrary]: Prop =
    serializeSequenceCompare[T](implicitly[Arbitrary[T]].arbitrary)

  def serializeSequenceEquiv[T: Serialization](g: Gen[T]): Prop = forAll(Gen.listOf(g)) { list =>
    // make sure the list is even in size:
    val pairList = (if (list.size % 2 == 1) list.tail else list).grouped(2)
    val baos1 = new ByteArrayOutputStream
    val baos2 = new ByteArrayOutputStream
    pairList.foreach {
      case Seq(a, b) =>
        Serialization.write(baos1, a)
        Serialization.write(baos2, b)
      case _ => sys.error("unreachable")
    }
    // now the compares must match:
    val in1 = baos1.toInputStream
    val in2 = baos2.toInputStream
    pairList.forall {
      case Seq(a, b) =>
        val rta = Serialization.read[T](in1).get
        val rtb = Serialization.read[T](in2).get
        Serialization.equiv(a, rta) && Serialization.equiv(b, rtb)
      case _ => sys.error("unreachable")
    }
  }
  def serializeSequenceEquiv[T: Serialization: Arbitrary]: Prop =
    serializeSequenceEquiv[T](implicitly[Arbitrary[T]].arbitrary)

  property("sequences compare well [Int]") = serializeSequenceCompare[Int]
  property("sequences equiv well [Int]") = serializeSequenceEquiv[Int]
  property("sequences compare well [(Int, Int)]") = serializeSequenceCompare[(Int, Int)]
  property("sequences equiv well [(Int, Int)]") = serializeSequenceEquiv[(Int, Int)]

  property("sequences compare well [String]") = serializeSequenceCompare[String]
  property("sequences equiv well [String]") = serializeSequenceEquiv[String]
  property("sequences compare well [(String, String)]") = serializeSequenceCompare[(String, String)]
  property("sequences equiv well [(String, String)]") = serializeSequenceEquiv[(String, String)]

  property("sequences compare well [IntWrapperClass]") = serializeSequenceCompare[IntWrapperClass]
  property("sequences compare well [IntTryWrapperClass]") = serializeSequenceCompare[IntTryWrapperClass]
  property("sequences equiv well [IntWrapperClass]") = serializeSequenceEquiv[IntWrapperClass]
  property("sequences equiv well [IntTryWrapperClass]") = serializeSequenceEquiv[IntTryWrapperClass]

  // Test the independent, non-sequenced, laws as well
  include(LawTester("Int Ordered", OrderedSerialization.allLaws[Int]))
  include(LawTester("(Int, Int) Ordered", OrderedSerialization.allLaws[(Int, Int)]))
  include(LawTester("String Ordered", OrderedSerialization.allLaws[String]))
  include(LawTester("(String, Int) Ordered", OrderedSerialization.allLaws[(String, Int)]))
  include(LawTester("(Int, String) Ordered", OrderedSerialization.allLaws[(Int, String)]))
  include(LawTester("(String, String) Ordered", OrderedSerialization.allLaws[(String, String)]))
  include(LawTester("IntWrapperClass Ordered", OrderedSerialization.allLaws[IntWrapperClass]))
  include(LawTester("IntTryWrapperClass Ordered", OrderedSerialization.allLaws[IntTryWrapperClass]))
}
