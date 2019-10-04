package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.parquet.thrift.{DecodingSchemaMismatchException, ThriftSchemaConverter}
import org.apache.parquet.thrift.struct.ThriftField.Requirement
import org.apache.parquet.thrift.struct.{ThriftField, ThriftType}
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType
import org.apache.parquet.thrift.struct.ThriftType.{ListType, MapType, StructType}
import org.scalatest.{Matchers, WordSpec}

class ParquetCollectionFormatCompatibilityTests extends WordSpec with Matchers {

  private def testProjectAndAssertCompatibility(fileSchema: MessageType,
                                                projectedReadSchema: MessageType) = {
    val projectedFileSchema = ParquetCollectionFormatCompatibility.projectFileSchema(fileSchema, projectedReadSchema)
    ScroogeReadSupport.assertGroupsAreCompatible(fileSchema, projectedFileSchema)
    projectedFileSchema
  }

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

  val listRequiredElementRules = Seq(
    ("element", listElementRule(_, _)),
    ("array", listArrayRule(_, _)),
    ("tuple", listTupleRule(_, _)),
    ("standard", (from: String, to: String) => listStandardRule(from, to, nullableElement = false))
  )

  // All possible format pairs of list with non-nullable element
  for {
    (projectedReadRuleName, projectedReadSchemaFunc) <- listRequiredElementRules
    (fileRuleName, fileSchemaFunc) <- listRequiredElementRules
  } yield {
    s"Project for list with non-nullable element file: [${fileRuleName}] read: [${projectedReadRuleName}]" should {
      "take option/require specifications from projected read schema" in {
        for {
          feasibleRepetition <- feasibleRepetitions
        } yield {
          testProjectedFileSchemaHasReadSchemaRepetitions(
            fileSchemaFunc,
            projectedReadSchemaFunc,
            feasibleRepetition
          )
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

  private def testProjectedFileSchemaHasReadSchemaRepetitions(
                                                               fileSchemaFunc: (String, String) => String,
                                                               projectedReadSchemaFunc: (String, String) => String,
                                                               feasibleRepetition: TestRepetitions): Any = {

    val projectedReadSchema = MessageTypeParser.parseMessageType(
      projectedReadSchemaFunc(
        feasibleRepetition.projectedReadRepetition1,
        feasibleRepetition.projectedReadRepetition2)
    )
    val fileSchema = MessageTypeParser.parseMessageType(
      fileSchemaFunc(
        feasibleRepetition.fileRepetition1,
        feasibleRepetition.fileRepetition2)
    )
    val expectedProjectedFileSchema = MessageTypeParser.parseMessageType(
      fileSchemaFunc(
        feasibleRepetition.projectedReadRepetition1,
        feasibleRepetition.projectedReadRepetition2)
    )
    expectedProjectedFileSchema shouldEqual testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
  }

  "Project for list with nullable element" should {
    "file: standard, read: spark legacy write, with nullable elements" in {
      for {
        feasibleRepetition <- feasibleRepetitions
      } yield {
        testProjectedFileSchemaHasReadSchemaRepetitions(
          fileSchemaFunc = listStandardRule(_, _, nullableElement = true),
          projectedReadSchemaFunc = listSparkLegacyNullableElementRule,
          feasibleRepetition
        )
      }
    }

    "failed to format file: required element, read: legacy write with nullable element" in {
      for {
        feasibleRepetition <- feasibleRepetitions
        (_, requiredElementSchemaFunc) <- listRequiredElementRules
      } yield {
        val e = intercept[IllegalArgumentException] {
          testProjectedFileSchemaHasReadSchemaRepetitions(
            fileSchemaFunc = listSparkLegacyNullableElementRule,
            projectedReadSchemaFunc = requiredElementSchemaFunc,
            feasibleRepetition
          )
        }
        e.getMessage should include("Spark legacy mode for nullable element cannot take required element")
      }
    }
  }

  "Project for map" should {
    "file/read identity" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, fileSchema)
      fileSchema shouldEqual projectedFileSchema
    }

    "file/read identity from thrift struct (string key, struct value)" in {
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

      val projectedFileSchema = testProjectAndAssertCompatibility(message, message)
      message shouldEqual projectedFileSchema
    }

    "file/read identity from thrift struct (string key, list string value)" in {
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

      val projectedFileSchema = testProjectAndAssertCompatibility(message, message)
      message shouldEqual projectedFileSchema
    }

    "file: standard key_value, read: legacy (MAP_KEY_VALUE)" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: legacy (MAP_KEY_VALUE), read: standard key_value" in {
      val fileSchema = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
          |  required group map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      required int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group map_field (MAP) {
          |    repeated group key_value {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group map_field (MAP) {
          |    repeated group map (MAP_KEY_VALUE) {
          |      required binary key (UTF8);
          |      optional int32 value;
          |    }
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "map of map, file: standard key_value, read: legacy (MAP_KEY_VALUE)" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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
      expected shouldEqual projectedFileSchema
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

  "Format compat for list" should {
    "file: primitive array, read: x_tuple" in {
      val fileSchema = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
          |  required group country_codes (LIST) {
          |    repeated binary array (UTF8);
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated binary array (UTF8);
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: primitive element, read: x_tuple" in {
      val fileSchema = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
          |  required group country_codes (LIST) {
          |    repeated binary element (UTF8);
          |  }
          |  required int32 x;
          |}
        """.stripMargin)
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated binary element (UTF8);
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: 3-level, read: x_tuple" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated binary country_codes_tuple (UTF8);
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: group array, read: nested x_tuple" in {
      val fileSchema = MessageTypeParser.parseMessageType(
        """
          |message FileSchema {
          |  required group foo (LIST) {
          |    repeated group array (LIST) {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
      """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      // note optional of result, and field rename
      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group foo (LIST) {
          |    repeated group array (LIST) {
          |      repeated binary array (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: nested 3-level, read: nested x_tuple" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      val expected = MessageTypeParser.parseMessageType(
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
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: 3-level, read: binary array" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |     repeated binary array (UTF8);
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated group list {
          |      required binary element (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      expected shouldEqual projectedFileSchema
    }

    "file: 3-level (identity), read: 3-level" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, fileSchema)
      fileSchema shouldEqual projectedFileSchema
    }

    "file: nested 3-level, read: nested primitive array" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)

      val expected = MessageTypeParser.parseMessageType(
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
      expected shouldEqual projectedFileSchema
    }

    "file: 3-level, read: element group" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group country_codes (LIST) {
          |    repeated group element {
          |      optional binary foo (UTF8);
          |      required binary zing (UTF8);
          |    }
          |  }
          |}
        """.stripMargin)
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)

      val expected = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
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
      expected shouldEqual projectedFileSchema
    }

    "file: nested primitive array, read: 3-level" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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

      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)

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
      expected shouldEqual projectedFileSchema
    }

    "file: 3-level, read: x_tuple in group" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
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
      expected shouldEqual projectedFileSchema
    }

    "file: x_tuple, read: 3-level" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group foo (LIST) {
          |    repeated group foo_tuple (LIST) {
          |      repeated binary foo_tuple_tuple (UTF8);
          |    }
          |  }
          |  optional int32 x;
          |}
        """.stripMargin) shouldEqual projectedFileSchema
    }
  }

  "Format compat for mixed collection" should {
    "list of map: file/read identity from thrift struct" in {
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

      val projectedFileSchema = testProjectAndAssertCompatibility(message, message)
      message shouldEqual projectedFileSchema
    }

    "map of list, file: standard, read: thrift-generated" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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

      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
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
      expected shouldEqual projectedFileSchema
    }

    "file: standard, read: list of map: tuple_x" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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

      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
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
      expected shouldEqual projectedFileSchema
    }

    "file: tuple_x, read: list of map: standard" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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

      val projectedFileSchema = testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
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
      expected shouldEqual projectedFileSchema
    }
  }

  "Format compat: check extra non-optional field projection" should {
    "throws on missing (MAP_KEY_VALUE) annotation causing projection of non-existent field" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
        testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      }

      e.getMessage should include("non-optional projected read field map:")
    }

    "throws on missing `repeated` causing projection of non-existent field" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
        testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      }

      e.getMessage should include("non-optional projected read field element:")
    }

    "throws on required but non-existent in target" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
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
        testProjectAndAssertCompatibility(fileSchema, projectedReadSchema)
      }

      e.getMessage should include("non-optional projected read field bogus_field:")
    }
  }

  "Schema mismatch" should {
    "throws exception on inconsistent type between primitive and group" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  required group foo {
          |    required binary bar (UTF8);
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        testProjectAndAssertCompatibility(projectedReadSchema, fileSchema)
      }

      e.getMessage should include("Found schema mismatch")
    }

    "throws exception optional group in file schema but required group in read schema" in {
      val fileSchema = MessageTypeParser.parseMessageType(
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
      val projectedReadSchema = MessageTypeParser.parseMessageType(
        """
          |message ProjectedReadSchema {
          |  optional group foo {
          |    required binary bar (UTF8);
          |  }
          |}
        """.stripMargin)

      val e = intercept[DecodingSchemaMismatchException] {
        testProjectAndAssertCompatibility(projectedReadSchema, fileSchema)
      }

      e.getMessage should include ("Found required projected read field foo")
      e.getMessage should include ("on optional file field")
    }
  }
}
