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
package com.twitter.scalding.db

import LittleEndianJavaStreamEnrichments._
import java.io._
import org.scalatest.{ FunSuite, ShouldMatchers }
import org.scalatest.prop.Checkers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Arbitrary, Gen, Prop }
import com.twitter.scalding._
import scala.collection.generic.CanBuildFrom

class LittleEndianJavaStreamEnrichmentsLawTests extends FunSuite with PropertyChecks with ShouldMatchers {

  def output = new ByteArrayOutputStream

  def writeRead[T](g: Gen[T], w: (T, OutputStream) => Unit, r: InputStream => T): Unit =
    forAll(g) { t =>
      val test = output
      w(t, test)
      assert(r(test.toInputStream) === t)
    }

  def writeRead[T: Arbitrary](w: (T, OutputStream) => Unit, r: InputStream => T): Unit =
    writeRead(implicitly[Arbitrary[T]].arbitrary, w, r)

  def writeToStream(w: OutputStream => Unit, len: Int): List[Int] = {
    val tmp = output
    w(tmp)
    val is = tmp.toInputStream
    (0 until len).map { _ => is.readUnsignedByte }.toList
  }

  test("Can (read/write)Float") {
    writeRead(
      { (i: Float, os) => os.writeFloat(i) }, { _.readFloat })
  }

  test("Can (read/write)Varchar") {
    writeRead(
      { (i: String, os) => os.writeVarchar(i) }, { _.readVarchar })
  }

  test("Can (read/write)Char") {
    forAll { p: (String, Short) =>
      val (str, delta) = p
      val absDelta = scala.math.abs(delta)
      val colSize = str.getBytes("UTF-8").size + absDelta
      val test = output
      test.writeChar(str, colSize)

      val res = test.toInputStream.readChar(colSize)
      assert(res.getBytes("UTF-8").size == colSize,
        s"""Should have returned back something the same size as our column, regardless of input.
          Passed in $str , got $res back""")
      // we trim since char may have zero padded the result
      assert(res.trim === str.trim)
    }
  }

  test("Can (read/write)Boolean") {
    writeRead(
      { (i: Boolean, os) => os.writeBoolean(i) }, { _.readBoolean })
  }

  test("Can (read/write)Double") {
    writeRead(
      { (i: Double, os) => os.writeDouble(i) }, { _.readDouble })
  }

  test("Can (read/write)Int") {
    writeRead(Gen.chooseNum(Int.MinValue, Int.MaxValue),
      { (i: Int, os) => os.writeInt(i) }, { _.readInt })
  }

  test("Can (read/write)Long") {
    writeRead(Gen.chooseNum(Long.MinValue, Long.MaxValue),
      { (i: Long, os) => os.writeLong(i) }, { _.readLong })
  }

  test("Can (read/write)Short") {
    writeRead(Gen.chooseNum(Short.MinValue, Short.MaxValue),
      { (i: Short, os) => os.writeShort(i) }, { _.readShort })
  }

  test("Can (read/write)UnsignedByte") {
    writeRead(Gen.chooseNum(0, (1 << 8) - 1),
      { (i: Int, os) => os.write(i.toByte) }, { _.readUnsignedByte })
  }

  test("Can (read/write)UnsignedShort") {
    writeRead(Gen.chooseNum(0, (1 << 16) - 1),
      { (i: Int, os) => os.writeShort(i.toShort) }, { _.readUnsignedShort })
  }

  // jan 1 200 => jan 1 2200
  test("Can (read/write)Timestamp") {
    writeRead(Gen.chooseNum(944006400000L, 7255440000000L),
      { (i: Long, os) => os.writeTimestampz(new java.util.Date(i)) }, { _.readTimestampz.getTime })
  }

  test("Can (read/write)Date") {
    writeRead(Gen.chooseNum(944006400000L, 7255440000000L).map{ ts =>
      ts - (ts % (1000L * 3600 * 24))
    },
      { (i: Long, os) => os.writeDate(new java.util.Date(i)) }, { _.readDate.getTime })
  }

  test("Matches expected float encoding") {
    val f: Double = -1.11
    val expected = List(0xC3, 0xF5, 0x28, 0x5C, 0x8F, 0xC2, 0xF1, 0xBF)
    val got = writeToStream(_.writeDouble(f), 8)

    assert(got === expected)
  }

  test("Matches expected Long encoding") {
    val f: Long = 1
    val expected = List(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
    val got = writeToStream(_.writeLong(f), 8)

    assert(got === expected)
  }

  test("Matches expected TimestampZ encoding") {
    implicit val tz = java.util.TimeZone.getTimeZone("UTC")
    implicit val dp = DateParser.default
    val scaldingDate = RichDate("1999-01-08 12:04:37")
    val expected = List(0x40, 0x1F, 0x3E, 0x64, 0xE8, 0xE3, 0xFF, 0xFF)

    assert(writeToStream(_.writeTimestampz(scaldingDate), 8) === expected)
  }

  test("Matches expected Date encoding") {
    implicit val tz = java.util.TimeZone.getTimeZone("UTC")
    implicit val dp = DateParser.default
    val scaldingDate = RichDate("1999-01-08")
    val expected = List(0x9A, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

    assert(writeToStream(_.writeDate(scaldingDate), 8) === expected)
  }

  test("Matches expected VarChar encoding") {
    val str = "ONE"
    val expected = List(0x03, 0x0, 0x00, 0x00, 0x4F, 0x4E, 0x45)

    assert(writeToStream(_.writeVarchar(str), expected.size) === expected)
  }

  test("Matches expected Char encoding") {
    val str = "one"
    val expected = List(0x6F, 0x6E, 0x65, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20)

    assert(writeToStream(_.writeChar(str, 10), expected.size) === expected)
  }
}
