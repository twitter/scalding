package com.twitter.scalding.parquet.scrooge

import org.apache.parquet.io.InvalidRecordException
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.{Matchers, WordSpec}

class ScroogeReadSupportTests extends WordSpec with Matchers {

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

}
