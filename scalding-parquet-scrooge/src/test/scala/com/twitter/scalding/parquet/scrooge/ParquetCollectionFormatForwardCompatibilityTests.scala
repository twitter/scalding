package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.thrift.ThriftSchemaConverter
import org.apache.parquet.thrift.struct.ThriftField.Requirement
import org.apache.parquet.thrift.struct.{ThriftField, ThriftType}
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType
import org.apache.parquet.thrift.struct.ThriftType.{ListType, MapType, StructType}
import org.scalatest.{Matchers, WordSpec}

class ParquetCollectionFormatForwardCompatibilityTests extends WordSpec with Matchers {

  "ScroogeReadSupport resolving map format" should {

    "map identity: " in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group map (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required group value {
          |        required binary _id (UTF8);
          |        optional double created;
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(fileType, fileType)
      projected shouldEqual fileType
    }

    "map identity: string key, struct value" in {
      val listType = new ListType(new ThriftField("list", 2, Requirement.REQUIRED, new ThriftType.StringType))
      val children = new ThriftField("foo", 3, Requirement.REQUIRED, listType)
      val mapValueType = new StructType(util.Arrays.asList(children),
        StructOrUnionType.STRUCT)
      val message = schemaFromThriftMap(mapValueType)
      message shouldEqual MessageTypeParser.parseMessageType(
        """
          |message ParquetSchema {
          |  required group map_field (MAP) = 6 {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional group value {
          |        required group foo (LIST) = 3 {
          |          repeated binary foo_tuple (UTF8);
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)

      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(message, message)
      projected shouldEqual message
    }

    "map identity: string kye, list string value" in {
      val listType = new ListType(new ThriftField("list", 2, Requirement.REQUIRED, new ThriftType.StringType))
      val message = schemaFromThriftMap(listType)
      message shouldEqual MessageTypeParser.parseMessageType(
        """
          |message ParquetSchema {
          |  required group map_field (MAP) = 6 {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional group value (LIST) {
          |        repeated binary value_tuple (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin
      )

      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(message, message)
      projected shouldEqual message
    }
  }

  private def schemaFromThriftMap(mapValueType: ThriftType) = {
    val mapType = new MapType(
      new ThriftField("NOT_USED_KEY", 4, Requirement.REQUIRED, new ThriftType.StringType),
      new ThriftField("NOT_USED_VALUE", 5, Requirement.REQUIRED,
        mapValueType)
    )
    new ThriftSchemaConverter().convert(
      new StructType(util.Arrays.asList(
        new ThriftField("map_field", 6, Requirement.REQUIRED, mapType)
      ), StructOrUnionType.STRUCT))
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)

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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(fileType, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)

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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)

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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)

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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
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

    "resolve does not support backward compat: project nested 3-level to x_tuple" in {
      val fileType = MessageTypeParser.parseMessageType(
        """
          |message scalding_schema {
          |  required group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val requestedProjection = MessageTypeParser.parseMessageType(
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
          |  optional int32 x;
          |}
        """.stripMargin)
      val projected = ParquetCollectionFormatForwardCompatibility.forwardCompatibleMessage(requestedProjection, fileType)
      projected shouldEqual requestedProjection
    }
  }
}
