package com.twitter.scalding.parquet.scrooge

import com.twitter.scalding.parquet.scrooge.thrift_scala.test.Address
import com.twitter.scalding.parquet.tuple.macros.Macros._
import com.twitter.scalding.parquet.tuple.{TypedParquet, TypedParquetSink}
import com.twitter.scalding.platform.{HadoopPlatformJobTest, HadoopSharedPlatformTest}
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Args, Job}
import org.apache.parquet.io.InvalidRecordException
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.{Matchers, WordSpec}

class ScroogeReadSupportTests extends WordSpec with Matchers with HadoopSharedPlatformTest {

  "ScroogeReadSupport getSchemaForRead" should {
    "project extra optional field" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message SampleClass {
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  required int32 x;
          |  optional int32 extra;
          |}
        """.stripMargin)

      val schema = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      schema shouldEqual requestedProjection
    }

    "fail projecting extra required field" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message SampleClass {
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  required int32 x;
          |  required int32 extra;
          |}
        """.stripMargin)

      an[InvalidRecordException] should be thrownBy {
        ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      }
    }

    "project required field using optional" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message SampleClass {
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional int32 x;
          |}
        """.stripMargin)

      val schema = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      schema shouldEqual requestedProjection
    }

    "fail projecting optional using required" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message SampleClass {
          |  optional int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  required int32 x;
          |}
        """.stripMargin)

      an[InvalidRecordException] should be thrownBy {
        ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      }
    }
  }

  "ScroogeReadSupport resolving list format" should {
    "resolve list legacy format: project x_tuple to legacy array" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated binary array (UTF8);
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated binary array (UTF8);
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve list legacy format: project x_tuple to 3-level" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve list legacy format: project nested x_tuple to nested legacy array" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group foo (LIST) {
          |    repeated group array (LIST) {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
      """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group array {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve list legacy format: project nested x_tuple to nested array" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group foo (LIST) {
          |    repeated group array (LIST) {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
      """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group array {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve list in group legacy format: project x_tuple to nested 3-level" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group foo (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated group list {
          |          required binary element (UTF8);
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated group list {
          |          required binary element (UTF8);
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve: binary array to 3-level nesting" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)

      // inner list is `binary array`
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |     repeated binary array (UTF8);
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected

    }

    "resolve: identity 3-level" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, fileType)
      projected shouldEqual fileType

    }

    "resolve nested list: project inner legacy array to 3-level nesting" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated group list {
          |          required binary element (UTF8);
          |        }
          |      }
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)

      // inner list is `binary array`
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated binary array (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated group list {
          |          required binary element (UTF8);
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve projected struct in list: repeated group element to 3-level nesting" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated group list {
          |      required group element {
          |        required binary foo (UTF8);
          |        required binary bar (UTF8);
          |        required binary zing (UTF8);
          |      }
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated group element {
          |      optional binary foo (UTF8);
          |      required binary zing (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required group element {
          |        optional binary foo (UTF8);
          |        required binary zing (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve standard 3-level list to 2-level" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message scalding_schema {
          |  required group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated binary array (UTF8);
          |      }
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)

      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated group list {
          |          required binary element (UTF8);
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated binary array (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve list in group containing list" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  optional group connect_delays (LIST) {
          |    repeated group list {
          |      required group element {
          |        optional binary description (UTF8);
          |        optional binary created_by (UTF8);
          |        optional group currencies (LIST) {
          |          repeated group list {
          |            required binary element (UTF8);
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group connect_delays (LIST) {
          |    repeated group connect_delays_tuple {
          |      optional binary description (UTF8);
          |      optional group currencies (LIST) {
          |        repeated binary currencies_tuple (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group connect_delays (LIST) {
          |    repeated group list {
          |      required group element {
          |        optional binary description (UTF8);
          |        optional group currencies (LIST) {
          |          repeated group list {
          |            required binary element (UTF8);
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }

    "resolve projection of different level" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  optional group foo (LIST) {
          |    repeated group list {
          |      required group element {
          |        required binary zing (UTF8);
          |        required binary bar (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group list {
          |      required group element {
          |        optional binary bar (UTF8);
          |        required binary zing (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ScroogeReadSupport.getSchemaForRead(fileType, requestedProjection)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleProjection {
          |  optional group foo (LIST) {
          |    repeated group list {
          |      required group element {
          |        optional binary bar (UTF8);
          |        required binary zing (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      projected shouldEqual expected
    }
  }

  "ScroogeReadSupport" should {
    "write using typedparquet and read using parquet scrooge" in {
      HadoopPlatformJobTest(new WriteToTypedParquetTupleJob(_), cluster)
        .arg("output", "output1")
        .sink[AddressCaseClass](TypedParquet[AddressCaseClass](Seq("output1"))) {
        in =>
          in should contain theSameElementsAs TypedParquetTestSources.caseClassValues
      }.run()

      HadoopPlatformJobTest(new ReadWithParquetScrooge(_), cluster)
        .arg("input", "output1")
        .arg("output", "output2")
        .sink[Address](new FixedPathParquetScrooge[Address]("output2")) {
        out =>
          out should contain theSameElementsAs TypedParquetTestSources.thriftValues
      }.run()
    }
  }

}

object TypedParquetTestSources {
  val thriftValues = Seq(Address("123 Embarcadero", "94111"), Address("123 E 79th St", "10075"), Address("456 W 80th St", "10075"))
  val caseClassValues = thriftValues.map(a => AddressCaseClass(a.street, a.zip))
}

case class AddressCaseClass(street: String, zip: String)

class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {
  val outputPath = args.required("output")
  val sink = TypedParquetSink[AddressCaseClass](outputPath)
  TypedPipe.from(TypedParquetTestSources.caseClassValues).write(sink)
}

class ReadWithParquetScrooge(args: Args) extends Job(args) {
  val inputPath = args.required("input")
  val outputPath = args.required("output")

  val input = new FixedPathParquetScrooge[Address](inputPath)
  val sink = new FixedPathParquetScrooge[Address](outputPath)
  TypedPipe.from(input).write(sink)
}