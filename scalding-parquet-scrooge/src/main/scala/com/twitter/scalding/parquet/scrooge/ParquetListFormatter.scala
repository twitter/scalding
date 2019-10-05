package com.twitter.scalding.parquet.scrooge

import org.apache.parquet.schema.{GroupType, OriginalType, PrimitiveType, Type}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Format parquet list schema of read type to structure of file type.
 * The supported formats are in `rules` of [[ParquetListFormatRule]].
 * Please see documentation for each rule.
 *
 * In a common use case, read schema form thrift struct has tuple format created by
 * [[org.apache.parquet.thrift.ThriftSchemaConvertVisitor]] which always suffix
 * list element with "_tuple".
 */
private[scrooge] object ParquetListFormatter extends ParquetCollectionFormatter {

  private val logger = LoggerFactory.getLogger(getClass)

  private val rules: Seq[ParquetListFormatRule] = Seq(
    PrimitiveElementRule,
    PrimitiveArrayRule,
    GroupElementRule,
    GroupArrayRule,
    TupleRule,
    StandardRule,
    SparkLegacyNullableElementRule
  )

  def formatCompatibleRepeatedType(fileRepeatedType: Type,
                                   readRepeatedType: Type,
                                   fieldContext: FieldContext,
                                   recursiveSolver: (Type, Type, FieldContext) => Type): Type = {
    (findRule(fileRepeatedType), findRule(readRepeatedType)) match {
      case (Some(fileRule), Some(readRule)) => {
        val readElementType = readRule.elementType(readRepeatedType)
        val fileElementType = fileRule.elementType(fileRepeatedType)
        val solvedElementType = recursiveSolver(fileElementType, readElementType, fieldContext)

        fileRule.createCompliantRepeatedType(
          elementType = solvedElementType,
          elementName = readRule.elementName(readRepeatedType),
          isElementRequired = readRule.isElementRequired(readRepeatedType),
          elementOriginalType = readRule.elementOriginalType(readRepeatedType),
          fieldContext = fieldContext
        )
      }

      case _ => readRepeatedType
    }
  }

  def extractGroup(typ: Type) : Option[ListGroup] = {
    if (isListGroup(typ)) {
      Some(ListGroup(typ.asGroupType(), typ.asGroupType().getFields.get(0)))
    } else {
      None
    }
  }

  private def isListGroup(typ: Type): Boolean = {
    if (typ.isPrimitive) {
      false
    } else {
      val groupProjection = typ.asGroupType
      groupProjection.getOriginalType == OriginalType.LIST &&
        groupProjection.getFieldCount == 1 &&
        groupProjection.getFields.get(0).isRepetition(Type.Repetition.REPEATED)
    }
  }

  private def findRule(repeatedType: Type): Option[ParquetListFormatRule] = {
    val ruleFound = rules.find(rule => rule.appliesToType(repeatedType))
    if (ruleFound.isEmpty) logger.warn(s"Unable to find matching rule for repeated type:\n$repeatedType")
    ruleFound
  }
}

/**
 * Rule allowing conversion from one format to other format by
 * 1) detect which format is the repeated list type.
 * 2) decompose the repeated type into element and other info.
 * 3) construct compliant repeated type from the given element and other info.
 * For example,
 * if read repeated type matches Rule 1, and file type matches Rule 2.
 * Rule 1 will decompose the read type, and
 * Rule 2 will take that information to construct repeated element in Rule 2 of file type format.
 */
private[scrooge] sealed trait ParquetListFormatRule {
  def elementType(repeatedType: Type): Type

  def elementName(repeatedType: Type): String = this.elementType(repeatedType).getName

  def elementOriginalType(repeatedType: Type): OriginalType = this.elementType(repeatedType).getOriginalType

  private[scrooge] def isElementRequired(repeatedType: Type): Boolean

  private[scrooge] def appliesToType(repeatedType: Type): Boolean

  private[scrooge] def createCompliantRepeatedType(elementType: Type,
                                                   elementName: String,
                                                   isElementRequired: Boolean,
                                                   elementOriginalType: OriginalType,
                                                   fieldContext: FieldContext): Type
}

/**
 * Rule 1 in https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
 * Although documentation only mentions `element` primitive and not for `array`,
 * Spark does write out with primitive `array` when legacy write format is enabled.
 * repeated int32 [element|array];
 */
private[scrooge] sealed trait PrimitiveListRule extends ParquetListFormatRule {

  def constantElementName: String

  override def elementType(repeatedType: Type): Type = repeatedType

  override private[scrooge] def isElementRequired(repeatedType: Type) = {
    // According to rule 1, "the repeated field is not a group,
    // then its type is the element type and elements are required."
    true
  }

  override def appliesToType(repeatedType: Type): Boolean =
    repeatedType.isPrimitive && repeatedType.getName == this.constantElementName

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType, fieldContext: FieldContext): Type = {
    if (!isElementRequired) throw new IllegalArgumentException(s"Primitive ${constantElementName} list format can only take required element")
    if (!typ.isPrimitive) throw new IllegalArgumentException(s"Primitive list format cannot take group, but is given $typ")
    new PrimitiveType(Type.Repetition.REPEATED, typ.asPrimitiveType.getPrimitiveTypeName, this.constantElementName, originalType)
  }
}

private[scrooge] object PrimitiveElementRule extends PrimitiveListRule {
  override def constantElementName: String = "element"
}

private[scrooge] object PrimitiveArrayRule extends PrimitiveListRule {
  override def constantElementName: String = "array"
}

/**
 * Rule 2 in https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
 * Although documentation only mentions `element` group and not for `array`,
 * Spark does write out with group `array` when legacy write format is enabled.
 * repeated group [element|array] {
 *   required binary str (UTF8);
 *   required int32 num;
 * }
 */
private[scrooge] sealed trait GroupListRule extends ParquetListFormatRule {

  def constantElementName: String

  override def isElementRequired(repeatedType: Type): Boolean = {
    // According Rule 2,
    // "If the repeated field is a group with multiple fields,
    // then its type is the element type and elements are required."
    true
  }

  override def elementType(repeatedType: Type): Type = repeatedType

  override def elementName(repeatedType: Type): String = this.constantElementName

  override def appliesToType(repeatedType: Type): Boolean = {
    if (repeatedType.isPrimitive) {
      false
    } else {
      val groupType = repeatedType.asGroupType
      groupType.getFields.size > 0 && groupType.getName == this.constantElementName
    }
  }

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType, fieldContext: FieldContext): Type = {
    if (!isElementRequired) throw new IllegalArgumentException(s"Group ${constantElementName} list format can only take required element")
    if (typ.isPrimitive) throw new IllegalArgumentException(s"Group list format cannot take primitive type, but is given $typ")
    else new GroupType(Type.Repetition.REPEATED, this.constantElementName, originalType, typ.asGroupType.getFields)
  }
}

private[scrooge] object GroupElementRule extends GroupListRule {
  override def constantElementName: String = "element"
}

private[scrooge] object GroupArrayRule extends GroupListRule {
  override def constantElementName: String = "array"
}

/**
 * Rule 3 in https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
 * Although the documentation only mentions group with one field, the generated schema from thrift struct
 * does write out both primitive type and group type with multiple fields.
 * repeated group my_list_field_tuple {
 *   required binary str (UTF8);
 * }
 * This repeated type implies the field name is `my_list_field`. This is the only format where
 * info is not fully self-contained.
 */
private[scrooge] object TupleRule extends ParquetListFormatRule {

  private val tupleSuffix = "_tuple"

  override def appliesToType(repeatedType: Type): Boolean = repeatedType.getName.endsWith(tupleSuffix)

  override def elementName(repeatedType: Type): String = {
    repeatedType.getName.substring(0, repeatedType.getName.length - tupleSuffix.length)
  }

  override def elementType(repeatedType: Type): Type = repeatedType

  override private[scrooge] def isElementRequired(repeatedType: Type) = {
    true
  }

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType, fieldContext: FieldContext): Type = {
    // nested list has type name of the form: `field_original_name_tuple_tuple..._tuple` for the depth of list
    val suffixed_name = (List(fieldContext.name) ++ (1 to fieldContext.nestedListLevel).toList.map(_ => "tuple")).mkString("_")
    if (typ.isPrimitive) {
      new PrimitiveType(Type.Repetition.REPEATED, typ.asPrimitiveType.getPrimitiveTypeName, suffixed_name, originalType)
    } else {
      new GroupType(Type.Repetition.REPEATED, suffixed_name, originalType, typ.asGroupType.getFields)
    }
  }
}


private[scrooge] sealed trait ThreeLevelRule extends ParquetListFormatRule {

  def constantElementName: String

  def constantRepeatedGroupName: String

  override def appliesToType(repeatedField: Type): Boolean = {
    if (repeatedField.isPrimitive || !(repeatedField.getName == constantRepeatedGroupName)) {
      false
    } else {
      elementType(repeatedField).getName == constantElementName
    }
  }

  override def elementType(repeatedType: Type): Type = firstField(repeatedType.asGroupType)

  override private[scrooge] def isElementRequired(repeatedType: Type): Boolean = {
    elementType(repeatedType).getRepetition == Type.Repetition.REQUIRED
  }

  override def elementName(repeatedType: Type): String = constantElementName

  override def createCompliantRepeatedType(originalElementType: Type, name: String, isElementRequired: Boolean, originalType: OriginalType, fieldContext: FieldContext): Type = {

    val repetition = if (isElementRequired) Type.Repetition.REQUIRED else Type.Repetition.OPTIONAL
    val elementType = if (originalElementType.isPrimitive) {
      new PrimitiveType(repetition, originalElementType.asPrimitiveType.getPrimitiveTypeName, constantElementName, originalType)
    } else {
      new GroupType(
        repetition,
        constantElementName,
        originalType,
        originalElementType.asGroupType.getFields)
    }

    new GroupType(Type.Repetition.REPEATED, constantRepeatedGroupName, Seq(elementType).asJava)
  }

  private def firstField(groupType: GroupType): Type = {
    groupType.getFields.get(0)
  }
}

/**
 * Standard parquet list format.
 * repeated group list {
 *   <element-repetition> <element-type> element;
 * }
 */
private[scrooge] object StandardRule extends ThreeLevelRule {

  def constantElementName = "element"

  def constantRepeatedGroupName = "list"
}

/**
 * Spark legacy format when element is nullable.
 * repeated group bag {
 *   optional <element-type> array;
 * }
 * Documentation on Spark is incorrect at the time of writing. It indicates `optional group bag`,
 * but it should be `repeated group bag`, and optional element.
 * https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L345-L355
 * Writing Dataset[Seq[List]] in Spark with default encoder and legacy mode on will give
 *
 * message spark_schema {
 *   optional group value (LIST) {
 *     repeated group bag {
 *       optional binary array (UTF8);
 *     }
 *   }
 * }
 */
private[scrooge] object SparkLegacyNullableElementRule extends ThreeLevelRule {
  override def constantElementName: String = "array"

  override def constantRepeatedGroupName: String = "bag"

  override def createCompliantRepeatedType(originalElementType: Type, name: String, isElementRequired: Boolean, originalType: OriginalType, fieldContext: FieldContext): Type = {
    if (isElementRequired) {
      throw new IllegalArgumentException(s"Spark legacy mode for nullable element cannot take required element. Found: ${originalElementType}")
    } else {
      super.createCompliantRepeatedType(originalElementType, name, isElementRequired, originalType, fieldContext)
    }
  }
}
