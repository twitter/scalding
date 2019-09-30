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