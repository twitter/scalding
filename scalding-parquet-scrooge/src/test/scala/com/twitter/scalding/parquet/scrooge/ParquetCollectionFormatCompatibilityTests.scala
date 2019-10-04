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

  /**
   * Helper wrapper to specify repetition string for exhaustive tests
   */
  case class TestRepetitions(projectedReadRepetition1: String, projectedReadRepetition2: String,
                             fileRepetition1: String, fileRepetition2: String)
  def feasibleRepetitions = {
    for {
      projectedRepetition1 <- Seq("required", "optional")
      projectedRepetition2 <- Seq("required", "optional")
      fileRepetition1 <- Seq("required", "optional")
      fileRepetition2 <- Seq("required", "optional")
      // when file type is optional, required projected type is breaking
      if !(fileRepetition1 == "optional" && projectedRepetition1 == "required")
      if !(fileRepetition2 == "optional" && projectedRepetition2 == "required")
    } yield {
      TestRepetitions(projectedRepetition1, projectedRepetition2, fileRepetition1, fileRepetition2)
    }
  }

  /**
   * The following functions of different list formats are equivalent schemas to describe:
   * {{
   *   x: Int
   *   foo_string_list: Seq[Int]
   *   foo_struct_list: Option[Seq[Struct]]
   *   foo_list_of_list: Seq[Seq[Long]]
   *   y: Int
   *   foo_optional_list:
   * }}
   */
  def listElementRule(repetition1: String, repetition2: String) = (
    s"""
      |message schema {
      |  $repetition1 int32 x;
      |  required group foo_string_list (LIST) {
      |    repeated int32 element;
      |  }
      |  optional group foo_struct_list (LIST) {
      |    repeated group element {
      |      required binary str (UTF8);
      |      ${repetition2} int32 num;
      |    }
      |  }
      |  required group foo_list_of_list (LIST) {
      |    repeated group element (LIST) {
      |      repeated int64 element;
      |    }
      |  }
      |  $repetition2 int32 y;
      |}
        """.stripMargin)

  def listArrayRule(repetition1: String, repetition2: String) = (
    s"""
       |message schema {
       |  $repetition1 int32 x;
       |  required group foo_string_list (LIST) {
       |    repeated int32 array;
       |  }
       |  optional group foo_struct_list (LIST) {
       |    repeated group array {
       |      required binary str (UTF8);
       |      ${repetition2} int32 num;
       |    }
       |  }
       |  required group foo_list_of_list (LIST) {
       |    repeated group array (LIST) {
       |      repeated int64 array;
       |    }
       |  }
       |  $repetition2 int32 y;
       |}
        """.stripMargin)

  def listTupleRule(repetition1: String, repetition2: String) = (
    s"""
       |message schema {
       |  $repetition1 int32 x;
       |  required group foo_string_list (LIST) {
       |    repeated int32 foo_string_list_tuple;
       |  }
       |  optional group foo_struct_list (LIST) {
       |    repeated group foo_struct_list_tuple {
       |      required binary str (UTF8);
       |      ${repetition2} int32 num;
       |    }
       |  }
       |  required group foo_list_of_list (LIST) {
       |    repeated group foo_list_of_list_tuple (LIST) {
       |      repeated int64 foo_list_of_list_tuple_tuple;
       |    }
       |  }
       |  $repetition2 int32 y;
       |}
        """.stripMargin)

  def listStandardRule(repetition1: String, repetition2: String, nullableElement:Boolean=false) = {
    val requiredOrOptional = if (nullableElement) "optional" else "required"
    (s"""
       |message schema {
       |  $repetition1 int32 x;
       |  required group foo_string_list (LIST) {
       |    repeated group list {
       |      $requiredOrOptional int32 element;
       |    }
       |  }
       |  optional group foo_struct_list (LIST) {
       |    repeated group list {
       |      $requiredOrOptional group element {
       |        required binary str (UTF8);
       |        ${repetition2} int32 num;
       |      }
       |    }
       |  }
       |  required group foo_list_of_list (LIST) {
       |    repeated group list {
       |      $requiredOrOptional group element (LIST) {
       |        repeated group list {
       |          $requiredOrOptional int64 element;
       |        }
       |      }
       |    }
       |  }
       |  $repetition2 int32 y;
       |}
        """.stripMargin)
  }

  val requiredElementRules = Seq(
    ("element", listElementRule(_, _)),
    ("array", listArrayRule(_, _)),
    ("tuple", listTupleRule(_, _)),
    ("standard", (from: String, to: String) => listStandardRule(from, to, nullableElement = false))
  )
  for {
    (projectedReadRuleName, projectedReadSchemaFunc) <- requiredElementRules
    (fileRuleName, fileSchemaFunc) <- requiredElementRules
  } yield {
    s"Format from: [${projectedReadRuleName}] to: [${fileRuleName}]" should {
      "take option/require specifications from projected read schema" in {
        for {
          feasibleRepetition <- feasibleRepetitions
        } yield {
          val projectedRepetition1 = feasibleRepetition.projectedReadRepetition1
          val projectedRepetition2 = feasibleRepetition.projectedReadRepetition2
          val projectedReadSchema = MessageTypeParser.parseMessageType(projectedReadSchemaFunc(projectedRepetition1, projectedRepetition2))

          val fileRepetition1 = feasibleRepetition.fileRepetition1
          val fileRepetition2 = feasibleRepetition.fileRepetition2
          val fileSchema = MessageTypeParser.parseMessageType(fileSchemaFunc(fileRepetition1, fileRepetition2))

          val expectedProjectedFileSchema = MessageTypeParser.parseMessageType(
            fileSchemaFunc(projectedRepetition1, projectedRepetition2))

          expectedProjectedFileSchema shouldEqual ParquetCollectionFormatForwardCompatibility
            .projectFileSchema(projectedReadSchema, fileSchema)
        }
      }
    }
  }

  def listSparkLegacyNullableElementRule(repetition1: String, repetition2: String) = (
    s"""
       |message schema {
       |  $repetition1 int32 x;
       |  required group foo_string_list (LIST) {
       |    repeated group bag {
       |      optional int32 array;
       |    }
       |  }
       |  optional group foo_struct_list (LIST) {
       |    repeated group bag {
       |      optional group array {
       |        required binary str (UTF8);
       |        ${repetition2} int32 num;
       |      }
       |    }
       |  }
       |  required group foo_list_of_list (LIST) {
       |    repeated group bag {
       |      optional group array (LIST) {
       |        repeated group bag {
       |          optional int64 array;
       |        }
       |      }
       |    }
       |  }
       |  $repetition2 int32 y;
       |}
        """.stripMargin)

  "Format compat for list with nullable element" should {
    "format from spark legacy write, with nullable elements, to standard" in {
      for {
        feasibleRepetition <- feasibleRepetitions
      } yield {
        val projectedRepetition1 = feasibleRepetition.projectedReadRepetition1
        val projectedRepetition2 = feasibleRepetition.projectedReadRepetition2
        val projectedReadSchema = MessageTypeParser.parseMessageType(listSparkLegacyNullableElementRule(projectedRepetition1, projectedRepetition2))

        val fileRepetition1 = feasibleRepetition.fileRepetition1
        val fileRepetition2 = feasibleRepetition.fileRepetition2
        val fileSchema = MessageTypeParser.parseMessageType(listStandardRule(fileRepetition1,
          fileRepetition2,
          nullableElement = true)
        )
        val expectedProjectedFileSchema = MessageTypeParser.parseMessageType(listStandardRule(projectedRepetition1, projectedRepetition2, nullableElement = true))
        expectedProjectedFileSchema shouldEqual ParquetCollectionFormatForwardCompatibility
          .projectFileSchema(projectedReadSchema, fileSchema)
      }
    }

    "failed to format required element to spark legacy write with nullable element" in {
      for {
        feasibleRepetition <- feasibleRepetitions
        (_, requiredElementSchemaFunc) <- requiredElementRules
      } yield {
        val projectedRepetition1 = feasibleRepetition.projectedReadRepetition1
        val projectedRepetition2 = feasibleRepetition.projectedReadRepetition2
        val fileSchema = MessageTypeParser.parseMessageType(listSparkLegacyNullableElementRule(projectedRepetition1, projectedRepetition2))

        val fileRepetition1 = feasibleRepetition.fileRepetition1
        val fileRepetition2 = feasibleRepetition.fileRepetition2
        val projectedReadSchema = MessageTypeParser.parseMessageType(requiredElementSchemaFunc(fileRepetition1,
          fileRepetition2)
        )
        val e = intercept[IllegalArgumentException] {
          ParquetCollectionFormatForwardCompatibility.projectFileSchema(projectedReadSchema, fileSchema)
        }
        e.getMessage should include("Spark legacy mode for nullable element cannot take required element")
      }
    }
  }

  "Format forward compat: resolving map format" should {
    "map identity" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(targetType, targetType)
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

      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(message, message)
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

      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(message, message)
      solved shouldEqual message
    }

    "format map legacy: original type (MAP_KEY_VALUE) to standard format key_value" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message SampleSource {
          |  optional group foo (LIST) {
          |    repeated group array (LIST) {
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)

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
          |message FileSchema {
          |  required group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(targetType, targetType)
      solved shouldEqual targetType
    }

    "format nested primitive array to nested 3-level" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)

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
          |message FileSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)

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
          |message FileSchema {
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
          |message ProjectedReadSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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
          |message FileSchema {
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
          |message ProjectedReadSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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

    "format 3-level to x_tuple" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
          |message ProjectedReadSchema {
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
      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
      solved shouldEqual MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |  optional int32 x;
          |}
        """.stripMargin)
    }
  }

  "Format forward compat: resolving mixed collection" should {
    "list of map identity from thrift struct" in {
      val mapType = new MapType(
        new ThriftField("NOT_USED_KEY", 4, Requirement.REQUIRED, new ThriftType.StringType),
        new ThriftField("NOT_USED_VALUE", 5, Requirement.REQUIRED, new ThriftType.I64Type))
      val message = new ThriftSchemaConverter().convert(
        new StructType(util.Arrays.asList(
          new ThriftField("list_of_map", 2, Requirement.REQUIRED, new ListType(
            new ThriftField("NOT_USED_ELEMENT", 2, Requirement.REQUIRED, mapType))
          )
        ), StructOrUnionType.STRUCT))

      message shouldEqual MessageTypeParser.parseMessageType(
        """
          |message ParquetSchema {
          |  required group list_of_map (LIST) = 2 {
          |    repeated group list_of_map_tuple (MAP) {
          |      repeated group map (MAP_KEY_VALUE) {
          |        required binary key (UTF8);
          |        optional int64 value;
          |      }
          |    }
          |  }
          |}
          |
        """.stripMargin
      )

      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(message, message)
      solved shouldEqual message
    }

    "format map of list" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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

      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
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

    "format list of map: tuple_x to standard" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
          |message ProjectedReadSchema {
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

      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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

    "format list of map: standard to tuple_x" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
          |  required group list_of_map (LIST) {
          |    repeated group list_of_map_tuple (MAP) {
          |      repeated group map (MAP_KEY_VALUE) {
          |        required binary key (UTF8);
          |        required group value {
          |          required binary _id (UTF8);
          |          required double created;
          |        }
          |      }
          |    }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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

      val solved = ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group list_of_map (LIST) {
          |    repeated group list_of_map_tuple (MAP) {
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
      solved shouldEqual expected
    }
  }

  "Format forward compat: check extra non-optional field projection" should {
    "throws on missing (MAP_KEY_VALUE) annotation causing projection of non-existent field" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
          |message ProjectedReadSchema {
          |  required group map_field (MAP) {
          |    repeated group map {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        ParquetCollectionFormatForwardCompatibility.projectFileSchema(
          sourceType,
          targetType
        )
      }

      e.getMessage should include("non-optional projected read field map:")
    }

    "throws on missing `repeated` causing projection of non-existent field" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
          |message ProjectedReadSchema {
          |  optional group foo (LIST) {
          |    required group element {
          |      optional binary zing (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        ParquetCollectionFormatForwardCompatibility.projectFileSchema(sourceType, targetType)
      }

      e.getMessage should include("non-optional projected read field element:")
    }

    "throws on required but non-existent in target" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
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
          |message ProjectedReadSchema {
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
        ParquetCollectionFormatForwardCompatibility.projectFileSchema(
          sourceType,
          targetType
        )
      }

      e.getMessage should include("non-optional projected read field bogus_field:")
    }
  }

  "Schema mismatch" should {
    "throws exception" in {
      val targetType = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
          |  required group foo {
          |    repeated group bar {
          |      required binary _id (UTF8);
          |      required double created;
          |     }
          |  }
          |}
        """.stripMargin)
      val sourceType = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group foo {
          |    required binary bar (UTF8);
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        ParquetCollectionFormatForwardCompatibility.projectFileSchema(
          targetType,
          sourceType
        )
      }

      e.getMessage should include("Found schema mismatch")
    }
  }
}
