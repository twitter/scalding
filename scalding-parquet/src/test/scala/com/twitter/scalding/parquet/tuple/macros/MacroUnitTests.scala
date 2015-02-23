package com.twitter.scalding.parquet.tuple.macros

import org.scalatest.{ Matchers, WordSpec }
import parquet.io.api.Binary
import parquet.schema.MessageTypeParser

case class SampleClassA(x: Int, y: String)

case class SampleClassB(a: SampleClassA, y: String)

case class SampleClassC(a: SampleClassA, b: SampleClassB)

case class SampleClassD(a: String, b: Boolean, c: Option[Short], d: Int, e: Long, f: Float, g: Option[Double])

case class SampleClassE(a: Int, b: Long, c: Short, d: Boolean, e: Float, f: Double, g: String)

case class SampleClassF(a: Option[SampleClassA])

case class SampleClassG(a: Int, b: Option[SampleClassF], c: Double)

class MacroUnitTests extends WordSpec with Matchers {

  "Macro generated case class parquet schema generator" should {

    "Generate parquet schema for SampleClassA" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassA])
      val expectedSchema = MessageTypeParser.parseMessageType("""
          |message SampleClassA {
          |  required int32 x;
          |  required binary y;
          |}
        """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassB" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassB])

      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassB {
        |  required group a {
        |    required int32 x;
        |    required binary y;
        |  }
        |  required binary y;
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassC" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassC])

      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassC {
        |  required group a {
        |    required int32 x;
        |    required binary y;
        |  }
        |  required group b {
        |    required group a {
        |      required int32 x;
        |      required binary y;
        |    }
        |    required binary y;
        |  }
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassD" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassD])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassD {
        |  required binary a;
        |  required boolean b;
        |  optional int32 c;
        |  required int32 d;
        |  required int64 e;
        |  required float f;
        |  optional double g;
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassE" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassE])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassE {
        |  required int32 a;
        |  required int64 b;
        |  required int32 c;
        |  required boolean d;
        |  required float e;
        |  required double f;
        |  required binary g;
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

  }

  "Macro generated case class field values generator" should {
    "Generate field values for SampleClassA" in {
      val a = SampleClassA(1, "foo")
      val values = Macros.caseClassFieldValues[SampleClassA]
      values(a) shouldEqual Map(0 -> 1, 1 -> "foo")
    }

    "Generate field values for SampleClassB with nested case class" in {
      val a = SampleClassA(1, "foo")
      val b = SampleClassB(a, "b")
      val values = Macros.caseClassFieldValues[SampleClassB]

      values(b) shouldEqual Map(0 -> 1, 1 -> "foo", 2 -> "b")
    }

    "Generate field values for SampleClassC with two nested case classes" in {

      val a = SampleClassA(1, "foo")

      val b = SampleClassB(a, "b")

      val c = SampleClassC(a, b)

      val values = Macros.caseClassFieldValues[SampleClassC]

      values(c) shouldEqual Map(0 -> 1, 1 -> "foo", 2 -> 1, 3 -> "foo", 4 -> "b")
    }

    "Generate field values for SampleClassD with option values" in {
      val d = SampleClassD("toto", b = true, Some(2), 1, 2L, 3F, Some(5D))
      val values = Macros.caseClassFieldValues[SampleClassD]
      values(d) shouldEqual Map(0 -> "toto", 1 -> true, 2 -> 2, 3 -> 1, 4 -> 2L, 5 -> 3F, 6 -> 5D)

      val d2 = SampleClassD("toto", b = true, None, 1, 2L, 3F, None)
      val values2 = Macros.caseClassFieldValues[SampleClassD]
      values2(d2) shouldEqual Map(0 -> "toto", 1 -> true, 3 -> 1, 4 -> 2L, 5 -> 3F)
    }

    "Generate field values for SampleClassF with optional nested case class " in {
      val a = SampleClassA(1, "foo")
      val f1 = SampleClassF(Some(a))
      val values1 = Macros.caseClassFieldValues[SampleClassF]
      values1(f1) shouldEqual Map(0 -> 1, 1 -> "foo")

      val f2 = SampleClassF(None)
      val values2 = Macros.caseClassFieldValues[SampleClassF]
      values2(f2) shouldEqual Map.empty
    }

    "Generate field values for SampleClassG with nested case class containing optional fields" in {
      val a = SampleClassA(1, "foo")
      val f1 = SampleClassF(Some(a))
      val g1 = SampleClassG(0, Some(f1), 1D)
      val values1 = Macros.caseClassFieldValues[SampleClassG]
      values1(g1) shouldEqual Map(0 -> 0, 1 -> 1, 2 -> "foo", 3 -> 1D)

      val f2 = SampleClassF(None)
      val g2 = SampleClassG(1, Some(f2), 2D)
      val values2 = Macros.caseClassFieldValues[SampleClassG]
      values2(g2) shouldEqual Map(0 -> 1, 3 -> 2D)

      val g3 = SampleClassG(1, None, 3D)
      val values3 = Macros.caseClassFieldValues[SampleClassG]
      values3(g3) shouldEqual Map(0 -> 1, 3 -> 3D)
    }
  }

  "Macro generated case class converters generator" should {

    "Generate converters for all primitive types" in {
      val converter = Macros.caseClassParquetTupleConverter[SampleClassE]

      val intConverter = converter.getConverter(0).asPrimitiveConverter()
      intConverter.addInt(0)

      val longConverter = converter.getConverter(1).asPrimitiveConverter()
      longConverter.addLong(1L)

      val shortConverter = converter.getConverter(2).asPrimitiveConverter()
      shortConverter.addInt(2)

      val boolean = converter.getConverter(3).asPrimitiveConverter()
      boolean.addBoolean(true)

      val float = converter.getConverter(4).asPrimitiveConverter()
      float.addFloat(3F)

      val double = converter.getConverter(5).asPrimitiveConverter()
      double.addDouble(4D)

      val string = converter.getConverter(6).asPrimitiveConverter()
      string.addBinary(Binary.fromString("foo"))

      converter.createValue shouldEqual SampleClassE(0, 1L, 2, d = true, 3F, 4D, "foo")
    }

    "Generate converters for case class with nested class" in {
      val converter = Macros.caseClassParquetTupleConverter[SampleClassB]

      val a = converter.getConverter(0).asGroupConverter()

      a.start()
      val aInt = a.getConverter(0).asPrimitiveConverter()
      aInt.addInt(2)
      val aString = a.getConverter(1).asPrimitiveConverter()
      aString.addBinary(Binary.fromString("foo"))
      a.end()

      val bString = converter.getConverter(1).asPrimitiveConverter()
      bString.addBinary(Binary.fromString("toto"))

      converter.createValue() shouldEqual SampleClassB(SampleClassA(2, "foo"), "toto")
    }

    "Generate converters for case class with optional nested class" in {
      val converter = Macros.caseClassParquetTupleConverter[SampleClassG]

      val a = converter.getConverter(0).asPrimitiveConverter()
      a.addInt(0)

      val b = converter.getConverter(1).asGroupConverter()
      b.start()
      val ba = b.getConverter(0).asGroupConverter()
      ba.start()
      val baInt = ba.getConverter(0).asPrimitiveConverter()
      baInt.addInt(2)
      val baString = ba.getConverter(1).asPrimitiveConverter()
      baString.addBinary(Binary.fromString("foo"))
      ba.end()
      b.end()

      val c = converter.getConverter(2).asPrimitiveConverter()
      c.addDouble(4D)

      converter.createValue() shouldEqual SampleClassG(0, Some(SampleClassF(Some(SampleClassA(2, "foo")))), 4D)
    }
  }
}