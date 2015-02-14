package com.twitter.scalding.parquet.tuple.macros

import org.scalatest.{ WordSpec, Matchers }
import parquet.schema.MessageTypeParser

case class SampleClassA(x: Int, y: String)
case class SampleClassB(a1: SampleClassA, a2: SampleClassA, y: String)
case class SampleClassC(a: SampleClassA, b: SampleClassB, c: SampleClassA, d: SampleClassB, e: SampleClassB)
case class SampleClassD(a: String, b: Boolean, c: Option[Short], d: Int, e: Long, f: Float, g: Option[Double])

class MacroUnitTests extends WordSpec with Matchers {

  "MacroGenerated Parquet schema generator" should {

    "Generate parquet schema for SampleClassA" in {
      val schema = Macros.caseClassParquetSchema[SampleClassA]
      val expectedSchema = MessageTypeParser.parseMessageType("""
          |message SampleClassA {
          |  required int32 x;
          |  required binary y;
          |}
        """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassB" in {
      val schema = Macros.caseClassParquetSchema[SampleClassB]

      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassB {
        |  required int32 a1.x;
        |  required binary a1.y;
        |  required int32 a2.x;
        |  required binary a2.y;
        |  required binary y;
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassC" in {
      val schema = Macros.caseClassParquetSchema[SampleClassC]

      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassC {
        |  required int32 a.x;
        |  required binary a.y;
        |  required int32 b.a1.x;
        |  required binary b.a1.y;
        |  required int32 b.a2.x;
        |  required binary b.a2.y;
        |  required binary b.y;
        |  required int32 c.x;
        |  required binary c.y;
        |  required int32 d.a1.x;
        |  required binary d.a1.y;
        |  required int32 d.a2.x;
        |  required binary d.a2.y;
        |  required binary d.y;
        |  required int32 e.a1.x;
        |  required binary e.a1.y;
        |  required int32 e.a2.x;
        |  required binary e.a2.y;
        |  required binary e.y;
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassD" in {
      val schema = Macros.caseClassParquetSchema[SampleClassD]
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
  }
}
