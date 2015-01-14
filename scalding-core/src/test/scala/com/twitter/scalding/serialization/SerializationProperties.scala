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
import scala.util.Try

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

  import OrderedSerialization.{ resultFrom, CompareFailure, readThenCompare }

  implicit val intOrderedSerialization: OrderedSerialization[Int] = new OrderedSerialization[Int] {
    def read(in: InputStream) = Try(Reader.read[Int](in))
    def write(o: OutputStream, t: Int) = Try(Writer.write[Int](o, t))
    def hash(t: Int) = t.hashCode
    def compare(a: Int, b: Int) = java.lang.Integer.compare(a, b)
    def compareBinary(a: InputStream, b: InputStream) =
      readThenCompare(a, b)(this)
  }

  implicit def tuple[A: OrderedSerialization, B: OrderedSerialization]: OrderedSerialization[(A, B)] =
    new OrderedSerialization2[A, B](implicitly, implicitly)

  include(LawTester("Int Ordered", OrderedSerialization.allLaws[Int]))
  include(LawTester("(Int, Int) Ordered", OrderedSerialization.allLaws[(Int, Int)]))
}
