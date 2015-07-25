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

import java.io.{ ByteArrayOutputStream, OutputStream }
import org.scalatest.WordSpec
import java.util.Date
import java.util.BitSet

import com.twitter.scalding.{ DateParser, RichDate }

case object MyValEncoder extends VerticaRowSerializer[MyVal] {
  import LittleEndianJavaStreamEnrichments._
  val NUM_FIELDS = 7

  def serialize(t: MyVal, o: OutputStream): Unit = {
    val bitSet = new BitSet(NUM_FIELDS)

    val baos = new ByteArrayOutputStream
    baos.writeLong(t.x)
    baos.writeDouble(t.y)
    baos.writeVarchar(t.z)
    baos.writeBoolean(t.a)
    if (t.k.isDefined) baos.writeLong(t.k.get)
    baos.writeDate(t.bDate)
    baos.writeTimestampz(t.cDateTime)

    o.writeInt(baos.size)
    o.writeByte((1 << 4).toByte)

    baos.writeTo(o)
  }
}

class VerticaNativeFormatTest extends WordSpec {
  "Vertica native" should {
    "Should have the correct header" in {
      val expectedHeader: Array[Byte] =
        List(
          0x4E, 0x41, 0x54, 0x49, 0x56, 0x45, 0x0A, // Signature
          0xFF, 0x0D, 0x0A, 0x00, // Signature
          0x1D, 0x00, 0x00, 0x00, // Length
          0x01, 0x00, // format version
          0x00, // filler
          0x06, 0x00, // number of columns
          0x08, 0x00, 0x00, 0x00, // Long
          0x08, 0x00, 0x00, 0x00, // Double
          0xFF, 0xFF, 0xFF, 0xFF, // Varchar
          0x01, 0x00, 0x00, 0x00, // Boolean
          0x08, 0x00, 0x00, 0x00, // Date
          0x08, 0x00, 0x00, 0x00 // TimestampZ
          ).map(_.toByte).toArray

      val expectedColumns = List(
        ColumnDefinition(BIGINT, ColumnName("bigInt"), NotNullable, None, None),
        ColumnDefinition(DOUBLE, ColumnName("double"), NotNullable, None, None),
        ColumnDefinition(VARCHAR, ColumnName("next"), NotNullable, None, None),
        ColumnDefinition(BOOLEAN, ColumnName("booleanFlag"), NotNullable, None, None),
        ColumnDefinition(DATE, ColumnName("smallString"), NotNullable, Some(20), None),
        ColumnDefinition(DATETIME, ColumnName("smallishString"), NotNullable, Some(200), None))

      val x = NativeVertica.headerFrom(expectedColumns)

      assert(x.size === expectedHeader.size, s"Expected size of ${expectedHeader.size}, got ${x.size}")
      assert(x === expectedHeader)
    }

    "Should have the expected row contents" in {

      implicit val tz = java.util.TimeZone.getTimeZone("UTC")
      implicit val dp = DateParser.default

      val t = MyVal(1L, -1.11, "ONE", true, None, RichDate("1999-01-08"), RichDate("1999-01-08T12:04:37"))
      val expected = List(
        0x28, 0x00, 0x00, 0x00,
        (1 << 4).toByte, // <8 fields, One is null
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1 in long
        0xC3, 0xF5, 0x28, 0x5C, 0x8F, 0xC2, 0xF1, 0xBF, // -1.11
        0x03, 0x00, 0x00, 0x00, 0x4F, 0x4E, 0x45, // "ONE"
        0x1, // Boolean true
        0x9A, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 1999-01-08
        0x40, 0x1F, 0x3E, 0x64, 0xE8, 0xE3, 0xFF, 0xFF // 1999-01-08T12:04:37
        ).map(_.toByte).toArray

      val baos = new ByteArrayOutputStream
      MyValEncoder.serialize(t, baos)
      val resBytes = baos.toByteArray

      // println("got\texpected")
      // resBytes.zip(expected).map {
      //   case (got, expected) =>
      //     println(s"${Integer.toHexString(got)}\t${Integer.toHexString(expected)}\t${got == expected}")
      // }
      assert(resBytes === expected)
    }
  }
}
