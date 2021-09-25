package com.twitter.scalding.parquet.tuple.macros

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import org.apache.parquet.io.api.{ Binary, RecordConsumer }
import org.apache.parquet.schema.MessageTypeParser

case class SampleClassA(x: Int, y: String)

case class SampleClassB(a: SampleClassA, y: String)

case class SampleClassC(a: SampleClassA, b: SampleClassB)

case class SampleClassD(a: String, b: Boolean, c: Option[Short], d: Int, e: Long, f: Float, g: Option[Double])

case class SampleClassE(a: Int, b: Long, c: Short, d: Boolean, e: Float, f: Double, g: String, h: Byte)

case class SampleClassF(a: Int, b: Option[SampleClassB], c: Double)

case class SampleClassG(a: Int, b: Option[List[Double]])

case class SampleClassH(a: Int, b: List[SampleClassA])

case class SampleClassI(a: Int, b: List[Option[Double]])

case class SampleClassJ(a: Map[Int, String])

case class SampleClassK(a: String, b: Map[SampleClassA, SampleClassB])

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

    "Generate parquet schema for SampleClassG" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassG])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassG {
        |  required int32 a;
        |  optional group b (LIST) {
        |    repeated group list {
        |      required double element;
        |    }
        |  }
        |}
        |
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassH" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassH])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassH {
        |  required int32 a;
        |  required group b (LIST) {
        |    repeated group list {
        |      required group element {
        |        required int32 x;
        |        required binary y;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassI" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassI])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassI {
        |  required int32 a;
        |  required group b (LIST) {
        |    repeated group list {
        |      optional double element;
        |    }
        |  }
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassJ" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassJ])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        |message SampleClassJ {
        |  required group a (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required binary value;
        |    }
        |  }
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }

    "Generate parquet schema for SampleClassK" in {
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassK])
      val expectedSchema = MessageTypeParser.parseMessageType("""
        message SampleClassK {
        |  required binary a;
        |  required group b (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required group key {
        |        required int32 x;
        |        required binary y;
        |      }
        |      required group value {
        |        required group a {
        |          required int32 x;
        |          required binary y;
        |        }
        |        required binary y;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
      schema shouldEqual expectedSchema
    }
  }

  "Macro case class converters generator" should {

    "Generate converters for all primitive types" in {
      val schema = Macros.caseClassParquetSchema[SampleClassE]
      val readSupport = Macros.caseClassParquetReadSupport[SampleClassE]
      val converter = readSupport.tupleConverter
      converter.start()
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
      converter.end()
      converter.currentValue shouldEqual SampleClassE(0, 1L, 2, d = true, 3F, 4D, "foo", 1)
    }

    "Generate converters for case class with nested class" in {
      val readSupport = Macros.caseClassParquetReadSupport[SampleClassB]
      val converter = readSupport.tupleConverter
      converter.start()
      val a = converter.getConverter(0).asGroupConverter()

      a.start()
      val aInt = a.getConverter(0).asPrimitiveConverter()
      aInt.addInt(2)
      val aString = a.getConverter(1).asPrimitiveConverter()
      aString.addBinary(Binary.fromString("foo"))
      a.end()

      val bString = converter.getConverter(1).asPrimitiveConverter()
      bString.addBinary(Binary.fromString("toto"))
      converter.end()
      converter.currentValue shouldEqual SampleClassB(SampleClassA(2, "foo"), "toto")
    }

    "Generate converters for case class with optional nested class" in {
      val readSupport = Macros.caseClassParquetReadSupport[SampleClassF]
      val converter = readSupport.tupleConverter
      converter.start()
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
      converter.end()
      converter.currentValue shouldEqual SampleClassF(0, Some(SampleClassB(SampleClassA(2, "foo"), "b1")), 4D)
    }

    "Generate converters for case class with list fields" in {
      val readSupport = Macros.caseClassParquetReadSupport[SampleClassH]
      val converter = readSupport.tupleConverter
      converter.start()
      val a = converter.getConverter(0).asPrimitiveConverter()
      a.addInt(0)

      val b = converter.getConverter(1).asGroupConverter()
      b.start()
      val ba = b.getConverter(0).asGroupConverter()
      ba.start()
      val baElement = ba.getConverter(0).asGroupConverter()
      baElement.start()
      val baInt = baElement.getConverter(0).asPrimitiveConverter()
      baInt.addInt(2)
      val baString = baElement.getConverter(1).asPrimitiveConverter()
      baString.addBinary(Binary.fromString("foo"))
      baElement.end()
      ba.end()
      b.end()
      converter.end()

      converter.currentValue shouldEqual SampleClassH(0, List(SampleClassA(2, "foo")))
    }

    "Generate converters for case class with map fields" in {
      val readSupport = Macros.caseClassParquetReadSupport[SampleClassK]
      val converter = readSupport.tupleConverter
      converter.start()
      val a = converter.getConverter(0).asPrimitiveConverter()
      a.addBinary(Binary.fromString("foo"))

      val keyValue = converter.getConverter(1).asGroupConverter().getConverter(0).asGroupConverter()
      keyValue.start()
      val key = keyValue.getConverter(0).asGroupConverter()
      key.start()
      val keyInt = key.getConverter(0).asPrimitiveConverter()
      keyInt.addInt(2)
      val keyString = key.getConverter(1).asPrimitiveConverter()
      keyString.addBinary(Binary.fromString("bar"))
      key.end()

      val value = keyValue.getConverter(1).asGroupConverter()
      value.start()
      val valueA = value.getConverter(0).asGroupConverter()
      valueA.start()
      val valueAInt = valueA.getConverter(0).asPrimitiveConverter()
      valueAInt.addInt(2)
      val valueAString = valueA.getConverter(1).asPrimitiveConverter()
      valueAString.addBinary(Binary.fromString("bar"))
      valueA.end()
      val valueString = value.getConverter(1).asPrimitiveConverter()
      valueString.addBinary(Binary.fromString("b1"))
      value.end()
      keyValue.end()
      converter.end()

      converter.currentValue shouldEqual SampleClassK("foo",
        Map(SampleClassA(2, "bar") -> SampleClassB(SampleClassA(2, "bar"), "b1")))
    }
  }

  "Macro case class parquet write support generator" should {
    "Generate write support for class with all the primitive type fields" in {

      val writeSupport = Macros.caseClassParquetWriteSupport[SampleClassE]
      val e = SampleClassE(0, 1L, 2, d = true, 3F, 4D, "foo", 1)
      val schema = Macros.caseClassParquetSchema[SampleClassE]
      val rc = new StringBuilderRecordConsumer
      writeSupport.writeRecord(e, rc, MessageTypeParser.parseMessageType(schema))

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
                                     |end message""".stripMargin

    }

    "Generate write support for nested case class and optional fields" in {
      val schemaString: String = Macros.caseClassParquetSchema[SampleClassF]
      val writeSupport = Macros.caseClassParquetWriteSupport[SampleClassF]

      val f = SampleClassF(0, Some(SampleClassB(SampleClassA(2, "foo"), "b1")), 4D)

      val schema = MessageTypeParser.parseMessageType(schemaString)
      val rc = new StringBuilderRecordConsumer
      writeSupport.writeRecord(f, rc, schema)

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
                                     |end message""".stripMargin

      //test write tuple with optional field = None
      val f2 = SampleClassF(0, None, 4D)
      val rc2 = new StringBuilderRecordConsumer
      writeSupport.writeRecord(f2, rc2, schema)
      rc2.writeScenario shouldEqual """start message
                                     |start field a at 0
                                     |write INT32 0
                                     |end field a at 0
                                     |start field c at 2
                                     |write DOUBLE 4.0
                                     |end field c at 2
                                     |end message""".stripMargin
    }

    "Generate write support for case class with LIST fields" in {
      //test write tuple with list of primitive fields
      val schemaString: String = Macros.caseClassParquetSchema[SampleClassI]
      val writeSupport = Macros.caseClassParquetWriteSupport[SampleClassI]
      val i = SampleClassI(0, List(None, Some(2)))
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassI])
      val rc = new StringBuilderRecordConsumer
      writeSupport.writeRecord(i, rc, schema)

      rc.writeScenario shouldEqual """start message
                                     |start field a at 0
                                     |write INT32 0
                                     |end field a at 0
                                     |start field b at 1
                                     |start group
                                     |start field list at 0
                                     |start group
                                     |end group
                                     |start group
                                     |start field element at 0
                                     |write DOUBLE 2.0
                                     |end field element at 0
                                     |end group
                                     |end field list at 0
                                     |end group
                                     |end field b at 1
                                     |end message""".stripMargin
      //test write list of nested class field
      val schemaString2: String = Macros.caseClassParquetSchema[SampleClassH]
      val writeSupport2 = Macros.caseClassParquetWriteSupport[SampleClassH]
      val h = SampleClassH(0, List(SampleClassA(2, "foo"), SampleClassA(2, "bar")))
      val schema2 = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassH])
      val rc2 = new StringBuilderRecordConsumer
      writeSupport2.writeRecord(h, rc2, schema2)

      rc2.writeScenario shouldEqual """start message
                                      |start field a at 0
                                      |write INT32 0
                                      |end field a at 0
                                      |start field b at 1
                                      |start group
                                      |start field list at 0
                                      |start group
                                      |start field element at 0
                                      |start group
                                      |start field x at 0
                                      |write INT32 2
                                      |end field x at 0
                                      |start field y at 1
                                      |write BINARY foo
                                      |end field y at 1
                                      |end group
                                      |end field element at 0
                                      |end group
                                      |start group
                                      |start field element at 0
                                      |start group
                                      |start field x at 0
                                      |write INT32 2
                                      |end field x at 0
                                      |start field y at 1
                                      |write BINARY bar
                                      |end field y at 1
                                      |end group
                                      |end field element at 0
                                      |end group
                                      |end field list at 0
                                      |end group
                                      |end field b at 1
                                      |end message""".stripMargin

    }

    "Generate write support for case class with MAP fields" in {
      //test write tuple with map of primitive fields
      val schemaString: String = Macros.caseClassParquetSchema[SampleClassJ]
      val writeSupport = Macros.caseClassParquetWriteSupport[SampleClassJ]

      val j = SampleClassJ(Map(1 -> "foo", 2 -> "bar"))
      val schema = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassJ])
      val rc = new StringBuilderRecordConsumer
      writeSupport.writeRecord(j, rc, schema)
      rc.writeScenario shouldEqual """start message
                                     |start field a at 0
                                     |start group
                                     |start field map at 0
                                     |start group
                                     |start field key at 0
                                     |write INT32 1
                                     |end field key at 0
                                     |start field value at 1
                                     |write BINARY foo
                                     |end field value at 1
                                     |end group
                                     |start group
                                     |start field key at 0
                                     |write INT32 2
                                     |end field key at 0
                                     |start field value at 1
                                     |write BINARY bar
                                     |end field value at 1
                                     |end group
                                     |end field map at 0
                                     |end group
                                     |end field a at 0
                                     |end message""".stripMargin

      //test write Map of case class field
      val schemaString2: String = Macros.caseClassParquetSchema[SampleClassK]
      val writeSupport2 = Macros.caseClassParquetWriteSupport[SampleClassK]
      val k = SampleClassK("foo", Map(SampleClassA(2, "foo") -> SampleClassB(SampleClassA(2, "foo"), "bar")))
      val schema2 = MessageTypeParser.parseMessageType(Macros.caseClassParquetSchema[SampleClassK])
      val rc2 = new StringBuilderRecordConsumer
      writeSupport2.writeRecord(k, rc2, schema2)

      rc2.writeScenario shouldEqual """start message
                                      |start field a at 0
                                      |write BINARY foo
                                      |end field a at 0
                                      |start field b at 1
                                      |start group
                                      |start field map at 0
                                      |start group
                                      |start field key at 0
                                      |start group
                                      |start field x at 0
                                      |write INT32 2
                                      |end field x at 0
                                      |start field y at 1
                                      |write BINARY foo
                                      |end field y at 1
                                      |end group
                                      |end field key at 0
                                      |start field value at 1
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
                                      |write BINARY bar
                                      |end field y at 1
                                      |end group
                                      |end field value at 1
                                      |end group
                                      |end field map at 0
                                      |end group
                                      |end field b at 1
                                      |end message""".stripMargin

    }
  }
}

//class to simulate record consumer for testing
class StringBuilderRecordConsumer extends RecordConsumer {
  val sb = new StringBuilder

  override def startMessage(): Unit = sb.append("start message\n")

  override def endMessage(): Unit = sb.append("end message")

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