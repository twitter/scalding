package com.twitter.scalding.parquet.tuple.macros

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import parquet.io.api.{ Binary, RecordConsumer }
import parquet.schema.MessageTypeParser

case class SampleClassA(x: Int, y: String)

case class SampleClassB(a: SampleClassA, y: String)

case class SampleClassC(a: SampleClassA, b: SampleClassB)

case class SampleClassD(a: String, b: Boolean, c: Option[Short], d: Int, e: Long, f: Float, g: Option[Double])

case class SampleClassE(a: Int, b: Long, c: Short, d: Boolean, e: Float, f: Double, g: String, h: Byte)

case class SampleClassF(a: Option[SampleClassA])

case class SampleClassG(a: Int, b: Option[SampleClassB], c: Double)

class MacroUnitTests extends WordSpec with Matchers with MockitoSugar {

  "Macro case class parquet schema generator" should {

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
        |  required int32 h;
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

  }

  "Macro case class converters generator" should {

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

      val byte = converter.getConverter(7).asPrimitiveConverter()
      byte.addInt(1)

      converter.createValue shouldEqual SampleClassE(0, 1L, 2, d = true, 3F, 4D, "foo", 1)
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

      val bString = b.getConverter(1).asPrimitiveConverter()
      bString.addBinary(Binary.fromString("b1"))
      b.end()

      val c = converter.getConverter(2).asPrimitiveConverter()
      c.addDouble(4D)

      converter.createValue() shouldEqual SampleClassG(0, Some(SampleClassB(SampleClassA(2, "foo"), "b1")), 4D)
    }
  }

  "Macro case class parquet write support generator" should {
    "Generate write support for class with all the primitive type fields" in {
      val writeSupportFn = Macros.caseClassWriteSupport[SampleClassE]
      val e = SampleClassE(0, 1L, 2, d = true, 3F, 4D, "foo", 1)
      val schema = Macros.caseClassParquetSchema[SampleClassE]
      val rc = new StringBuilderRecordConsumer
      writeSupportFn(e, rc, MessageTypeParser.parseMessageType(schema))

      rc.writeScenario shouldEqual """start message
                                     |start field a at 0
                                     |write INT32 0
                                     |end field a at 0
                                     |start field b at 1
                                     |write INT64 1
                                     |start message
                                     |start field a at 0
                                     |write INT32 0
                                     |end field a at 0
                                     |start field b at 1
                                     |write INT64 1
                                     |end field b at 1
                                     |start field c at 2
                                     |write INT32 2
                                     |end field c at 2
                                     |start field d at 3
                                     |write BOOLEAN true
                                     |end field d at 3
                                     |start field e at 4
                                     |write FLOAT 3.0
                                     |end field e at 4
                                     |start field f at 5
                                     |write DOUBLE 4.0
                                     |end field f at 5
                                     |start field g at 6
                                     |write BINARY foo
                                     |end field g at 6
                                     |start field h at 7
                                     |write INT32 1
                                     |end field h at 7
                                     |end Message""".stripMargin

    }

    "Generate write support for nested case class and optinal fields" in {
      val writeSupportFn = Macros.caseClassWriteSupport[SampleClassG]

      val g = SampleClassG(0, Some(SampleClassB(SampleClassA(2, "foo"), "b1")), 4D)

      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassG])
      val rc = new StringBuilderRecordConsumer
      writeSupportFn(g, rc, schema)

      rc.writeScenario shouldEqual """start message
                                     |start field a at 0
                                     |write INT32 0
                                     |end field a at 0
                                     |start field b at 1
                                     |start group
                                     |start field a at 0
                                     |start group
                                     |start field x at 0
                                     |write INT32 2
                                     |end field x at 0
                                     |start field y at 1
                                     |write BINARY foo
                                     |end field y at 1
                                     |end group
                                     |end field a at 0
                                     |start field y at 1
                                     |write BINARY b1
                                     |end field y at 1
                                     |end group
                                     |end field b at 1
                                     |start field c at 2
                                     |write DOUBLE 4.0
                                     |end field c at 2
                                     |end Message""".stripMargin

      //test write tuple with optional field = None
      val g2 = SampleClassG(0, None, 4D)
      val rc2 = new StringBuilderRecordConsumer
      writeSupportFn(g2, rc2, schema)
      rc2.writeScenario shouldEqual """start message
                                     |start field a at 0
                                     |write INT32 0
                                     |end field a at 0
                                     |start field c at 2
                                     |write DOUBLE 4.0
                                     |end field c at 2
                                     |end Message""".stripMargin
    }
  }
}

//class to simulate record consumer for testing
class StringBuilderRecordConsumer extends RecordConsumer {
  val sb = new StringBuilder

  override def startMessage(): Unit = sb.append("start message\n")

  override def endMessage(): Unit = sb.append("end Message")

  override def addFloat(v: Float): Unit = sb.append(s"write FLOAT $v\n")

  override def addBinary(binary: Binary): Unit = sb.append(s"write BINARY ${binary.toStringUsingUTF8}\n")

  override def addDouble(v: Double): Unit = sb.append(s"write DOUBLE $v\n")

  override def endGroup(): Unit = sb.append("end group\n")

  override def endField(s: String, i: Int): Unit = sb.append(s"end field $s at $i\n")

  override def startGroup(): Unit = sb.append("start group\n")

  override def startField(s: String, i: Int): Unit = sb.append(s"start field $s at $i\n")

  override def addBoolean(b: Boolean): Unit = sb.append(s"write BOOLEAN $b\n")

  override def addLong(l: Long): Unit = sb.append(sb.append(s"write INT64 $l\n"))

  override def addInteger(i: Int): Unit = sb.append(s"write INT32 $i\n")

  def writeScenario = sb.toString()
}