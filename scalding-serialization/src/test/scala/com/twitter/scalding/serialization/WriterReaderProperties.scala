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

import scala.collection.generic.CanBuildFrom

object WriterReaderProperties extends Properties("WriterReaderProperties") {

  def output = new ByteArrayOutputStream

  // The default Array[Equiv] is reference. WAT!?
  implicit def aeq[T: Equiv]: Equiv[Array[T]] = new Equiv[Array[T]] {
    def equiv(a: Array[T], b: Array[T]): Boolean = {
      val teq = Equiv[T]
      @annotation.tailrec
      def go(pos: Int): Boolean =
        if (pos == a.length) true
        else {
          teq.equiv(a(pos), b(pos)) && go(pos + 1)
        }

      (a.length == b.length) && go(0)
    }
  }
  implicit def teq[T1: Equiv, T2: Equiv]: Equiv[(T1, T2)] = new Equiv[(T1, T2)] {
    def equiv(a: (T1, T2), b: (T1, T2)) = {
      Equiv[T1].equiv(a._1, b._1) &&
        Equiv[T2].equiv(a._2, b._2)
    }
  }

  def writerReader[T: Writer: Reader: Equiv](g: Gen[T]): Prop =
    forAll(g) { t =>
      val test = output
      Writer.write(test, t)
      Equiv[T].equiv(Reader.read(test.toInputStream), t)
    }
  def writerReader[T: Writer: Reader: Equiv: Arbitrary]: Prop =
    writerReader(implicitly[Arbitrary[T]].arbitrary)

  def writerReaderCollection[T: Writer: Reader, C <: Iterable[T]: Arbitrary: Equiv](implicit cbf: CanBuildFrom[Nothing, T, C]): Prop =
    {
      implicit val cwriter: Writer[C] = Writer.collection[T, C]
      implicit val creader: Reader[C] = Reader.collection[T, C]
      writerReader(implicitly[Arbitrary[C]].arbitrary)
    }

  /*
   * Test the Writer/Reader type-classes
   */
  property("Unit Writer/Reader") = writerReader[Unit]
  property("Boolean Writer/Reader") = writerReader[Boolean]
  property("Byte Writer/Reader") = writerReader[Byte]
  property("Short Writer/Reader") = writerReader[Short]
  property("Int Writer/Reader") = writerReader[Int]
  property("Long Writer/Reader") = writerReader[Long]
  property("Float Writer/Reader") = writerReader[Float]
  property("Double Writer/Reader") = writerReader[Double]
  property("String Writer/Reader") = writerReader[String]
  property("Array[Byte] Writer/Reader") = writerReader[Array[Byte]]
  property("Array[Int] Writer/Reader") = writerReader[Array[Int]]
  property("Array[String] Writer/Reader") = writerReader[Array[String]]
  property("List[String] Writer/Reader") =
    writerReaderCollection[String, List[String]]
  property("(Int, Array[String]) Writer/Reader") =
    writerReader[(Int, Array[String])]

  property("Option[(Int, Double)] Writer/Reader") =
    writerReader[Option[(Int, Double)]]

  property("Option[Option[Unit]] Writer/Reader") =
    writerReader[Option[Option[Unit]]]

  property("Either[Int, String] Writer/Reader") =
    writerReader[Either[Int, String]]

  property("Map[Long, Byte] Writer/Reader") =
    writerReaderCollection[(Long, Byte), Map[Long, Byte]]
}
