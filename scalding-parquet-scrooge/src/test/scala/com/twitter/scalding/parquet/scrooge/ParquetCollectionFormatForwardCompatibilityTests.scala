package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.thrift.{DecodingSchemaMismatchException, ThriftSchemaConverter}
import org.apache.parquet.thrift.struct.ThriftField.Requirement
import org.apache.parquet.thrift.struct.{ThriftField, ThriftType}
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType
import org.apache.parquet.thrift.struct.ThriftType.{ListType, MapType, StructType}
import org.scalatest.{Matchers, WordSpec}

class ParquetCollectionFormatForwardCompatibilityTests extends WordSpec with Matchers {

  "Format forward compat: resolving map format" should {
    "map identity" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(targetType, targetType)
      solved shouldEqual targetType
    }

    "map identity from thrift struct: string key, struct value" in {
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

      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(message, message)
      solved shouldEqual message
    }

    "map identity from thrift struct: string kye, list string value" in {
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

      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(message, message)
      solved shouldEqual message
    }

    "format map legacy: original type (MAP_KEY_VALUE) to standard format key_value" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format map legacy map of map" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group map_of_map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required group value (MAP) {
          |        repeated group key_value {
          |          required binary key (UTF8);
          |          required group value {
          |            required binary _id (UTF8);
          |            required int32 x;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group map_of_map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      required group value (MAP) {
          |        repeated group map (MAP_KEY_VALUE) {
          |          required binary key (UTF8);
          |          required group value {
          |            optional int32 x;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group map_of_map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required group value (MAP) {
          |        repeated group key_value {
          |          required binary key (UTF8);
          |          required group value {
          |            optional int32 x;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    def schemaFromThriftMap(mapValueType: ThriftType) = {
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
  }

  "Format forward compat: resolving list format" should {
    "format x_tuple to primitive array" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated binary array (UTF8);
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated binary array (UTF8);
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format x_tuple to primitive element" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group country_codes (LIST) {
          |    repeated binary element (UTF8);
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated binary element (UTF8);
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format x_tuple to 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format nested x_tuple to group array" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group foo (LIST) {
          |    repeated group array (LIST) {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
      """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group foo (LIST) {
          |    repeated group array {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format nested x_tuple to nested 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      solved shouldEqual expected
    }

    "format binary array to 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |     repeated binary array (UTF8);
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected

    }

    "format 3-level to 3-level (identity)" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(targetType, targetType)
      solved shouldEqual targetType
    }

    "format nested primitive array to nested 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated binary array (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      solved shouldEqual expected
    }

    "format element group to 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group country_codes (LIST) {
          |    repeated group element {
          |      optional binary foo (UTF8);
          |      required binary zing (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      solved shouldEqual expected
    }

    "format 3-level to nested primitive array" in {
      val targetType = MessageTypeParser.parseMessageType(
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

      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group array_of_country_codes (LIST) {
          |    repeated group list {
          |      required group element (LIST) {
          |        repeated binary array (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format x_tuple in group to 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      solved shouldEqual expected
    }

    "does not format 3-level to x_tuple" in {
      val targetType = MessageTypeParser.parseMessageType(
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
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
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
      val solved = ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      solved shouldEqual sourceType
    }
  }

  "Format forward compat: resolving mixed collection" should {
    "format map of list" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required group value {
          |        optional group foo (LIST) {
          |          repeated group list {
          |            required binary element (UTF8);
          |          }
          |        }
          |        required int32 x;
          |      }
          |    }
          |  }
          |}
          |
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message ParquetSchema {
          |  required group map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional group value {
          |        optional group foo (LIST) {
          |          repeated binary foo_tuple (UTF8);
          |        }
          |        optional int32 x;
          |      }
          |    }
          |  }
          |}
        """.stripMargin)

      val solved = ScroogeReadSupport.getSchemaForRead(targetType, sourceType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ParquetSchema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      optional group value {
          |        optional group foo (LIST) {
          |          repeated group list {
          |            required binary element (UTF8);
          |          }
          |        }
          |        optional int32 x;
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }

    "format list of map" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group list_of_map (LIST) {
          |    repeated group list {
          |      required group element (MAP) {
          |        repeated group key_value {
          |          required binary key (UTF8);
          |          required group value {
          |            required binary _id (UTF8);
          |            required double created;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group list_of_map (LIST) {
          |    repeated group element_tuple (MAP) {
          |      repeated group map (MAP_KEY_VALUE) {
          |        required binary key (UTF8);
          |        optional group value {
          |          optional double created;
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)

      val solved = ScroogeReadSupport.getSchemaForRead(targetType, sourceType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group list_of_map (LIST) {
          |    repeated group list {
          |      required group element (MAP) {
          |        repeated group key_value {
          |          required binary key (UTF8);
          |          optional group value {
          |            optional double created;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      solved shouldEqual expected
    }
  }

  "Format forward compat: check extra non-optional field projection" should {
    "throws on missing (MAP_KEY_VALUE) annotation causing projection of non-existent field" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      // `map` isn't annotated with `MAP_KEY_VALUE`, and is thus treated as
      // an actual field which then fails projection
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group map_field (MAP) {
          |    repeated group map {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(
          sourceType,
          targetType
        )
      }

      e.getMessage should include("non-optional source field map:")
    }

    "throws on missing `repeated` causing projection of non-existent field" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  optional group foo (LIST) {
          |    repeated group list {
          |      required group element {
          |        required binary zing (UTF8);
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group foo (LIST) {
          |    required group element {
          |      optional binary zing (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(sourceType, targetType)
      }

      e.getMessage should include("non-optional source field element:")
    }

    "throws on required but non-existent in target" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message spark_schema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      required int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  required group map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional int32 value;
          |      required int32 bogus_field;
          |    }
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        ParquetCollectionFormatForwardCompatibility.formatForwardCompatibleMessage(
          sourceType,
          targetType
        )
      }

      e.getMessage should include("non-optional source field bogus_field:")
    }
  }
}
