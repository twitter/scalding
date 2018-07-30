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

object JavaStreamEnrichmentsProperties extends Properties("JavaStreamEnrichmentsProperties") {

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

  def writeRead[T: Equiv](g: Gen[T], w: (T, OutputStream) => Unit, r: InputStream => T): Prop =
    forAll(g) { t =>
      val test = output
      w(t, test)
      Equiv[T].equiv(r(test.toInputStream), t)
    }
  def writeRead[T: Equiv: Arbitrary](w: (T, OutputStream) => Unit, r: InputStream => T): Prop =
    writeRead(implicitly[Arbitrary[T]].arbitrary, w, r)

  property("Can (read/write)Size") = writeRead(Gen.chooseNum(0, Int.MaxValue),
    { (i: Int, os) => os.writePosVarInt(i) }, { _.readPosVarInt })

  property("Can (read/write)Float") = writeRead(
    { (i: Float, os) => os.writeFloat(i) }, { _.readFloat })

  property("Can (read/write)Array[Byte]") = writeRead(
    // Use list because Array has a shitty toString
    { (b: List[Byte], os) => os.writePosVarInt(b.size); os.writeBytes(b.toArray) },
    { is =>
      val bytes = new Array[Byte](is.readPosVarInt)
      is.readFully(bytes)
      bytes.toList
    })

  property("Can (read/write)Boolean") = writeRead(
    { (i: Boolean, os) => os.writeBoolean(i) }, { _.readBoolean })

  property("Can (read/write)Double") = writeRead(
    { (i: Double, os) => os.writeDouble(i) }, { _.readDouble })

  property("Can (read/write)Int") = writeRead(Gen.chooseNum(Int.MinValue, Int.MaxValue),
    { (i: Int, os) => os.writeInt(i) }, { _.readInt })

  property("Can (read/write)Long") = writeRead(Gen.chooseNum(Long.MinValue, Long.MaxValue),
    { (i: Long, os) => os.writeLong(i) }, { _.readLong })

  property("Can (read/write)Short") = writeRead(Gen.chooseNum(Short.MinValue, Short.MaxValue),
    { (i: Short, os) => os.writeShort(i) }, { _.readShort })

  property("Can (read/write)UnsignedByte") = writeRead(Gen.chooseNum(0, (1 << 8) - 1),
    { (i: Int, os) => os.write(i.toByte) }, { _.readUnsignedByte })

  property("Can (read/write)UnsignedShort") = writeRead(Gen.chooseNum(0, (1 << 16) - 1),
    { (i: Int, os) => os.writeShort(i.toShort) }, { _.readUnsignedShort })
}
