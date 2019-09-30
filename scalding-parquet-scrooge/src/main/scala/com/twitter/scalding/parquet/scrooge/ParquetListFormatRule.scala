package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.{GroupType, OriginalType, PrimitiveType, Type}

object ParquetListFormatRule {
  def isGroupList(projection: Type): Boolean = {
    if (projection.isPrimitive) return false
    val groupProjection = projection.asGroupType
    (groupProjection.getOriginalType eq OriginalType.LIST) && groupProjection.getFieldCount == 1 && groupProjection.getFields.get(0).isRepetition(Type.Repetition.REPEATED)
  }
}

private[scrooge] sealed trait ParquetListFormatRule {
  def elementType(repeatedType: Type): Type

  def elementName(repeatedType: Type): String = this.elementType(repeatedType).getName

  def elementOriginalType(repeatedType: Type): OriginalType = this.elementType(repeatedType).getOriginalType

  private[scrooge] def isElementRequired(repeatedType: Type): Boolean

  private[scrooge] def check(typ: Type): Boolean

  private[scrooge] def createCompliantRepeatedType(typ: Type,
                                                   name: String,
                                                   isElementRequired: Boolean,
                                                   originalType: OriginalType): Type
}

/**
 * repeated int32 [element|array];
 */
private[scrooge] sealed trait PrimitiveListRule extends ParquetListFormatRule {

  def constantElementName: String

  override def elementType(repeatedType: Type): Type = repeatedType

  override private[scrooge] def isElementRequired(repeatedType: Type) = {
    // According to Rule 1 from,
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
    // "the repeated field is not a group,
    // then its type is the element type and elements are required."
    true
  }

  override def check(repeatedType: Type): Boolean =
    repeatedType.isPrimitive && repeatedType.getName == this.constantElementName

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType): Type = {
    if (!isElementRequired) throw new IllegalArgumentException("Primitive list format can only take required element")
    if (!typ.isPrimitive) throw new IllegalArgumentException(String.format("Primitive list format cannot take group, but is given %s", typ))
    new PrimitiveType(Type.Repetition.REPEATED, typ.asPrimitiveType.getPrimitiveTypeName, this.constantElementName, originalType)
  }
}

object PrimitiveElementRule extends PrimitiveListRule {
  override def constantElementName: String = "element"
}

object PrimitiveArrayRule extends PrimitiveListRule {
  override def constantElementName: String = "array"
}

/**
 * repeated group [element|array] {
 *   required binary str (UTF8);
 *   required int32 num;
 * }
 */
private[scrooge] sealed trait GroupListRule extends ParquetListFormatRule {

  def constantElementName: String

  override def isElementRequired(repeatedType: Type): Boolean = {
    // According Rule 2 from
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
    // "If the repeated field is a group with multiple fields,
    // then its type is the element type and elements are required."
    true
  }

  override def elementType(repeatedType: Type): Type = repeatedType

  override def elementName(repeatedType: Type): String = this.constantElementName

  override def check(repeatedType: Type): Boolean = {
    if (repeatedType.isPrimitive) false
    else {
      val groupType = repeatedType.asGroupType
      groupType.getFields.size > 0 && groupType.getName == this.constantElementName
    }
  }

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType): Type = {
    if (typ.isPrimitive) new GroupType(Type.Repetition.REPEATED, this.constantElementName, typ)
    else new GroupType(Type.Repetition.REPEATED, this.constantElementName, typ.asGroupType.getFields)
  }
}

object GroupElementRule extends GroupListRule {
  override def constantElementName: String = "element"
}

object GroupArrayRule extends GroupListRule {
  override def constantElementName: String = "array"
}

object TupleRule extends ParquetListFormatRule {
  override def check(repeatedType: Type): Boolean = repeatedType.getName.endsWith("_tuple")

  override def elementName(repeatedType: Type): String = {
    repeatedType.getName.substring(0, repeatedType.getName.length - 6)
  }

  override def elementType(repeatedType: Type): Type = repeatedType

  override private[scrooge] def isElementRequired(repeatedType: Type) = {
    // According to Rule 3 from
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
    true
  }

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType): Type = {
    val suffixed_name = name + "_tuple"
    if (typ.isPrimitive) new PrimitiveType(Type.Repetition.REPEATED, typ.asPrimitiveType.getPrimitiveTypeName, suffixed_name, originalType)
    else new GroupType(Type.Repetition.REPEATED, suffixed_name, OriginalType.LIST, typ.asGroupType.getFields)
  }
}

object StandardRule extends ParquetListFormatRule {
  /**
   * repeated group list {
   *   <element-repetition> <element-type> element;
   * }
   */
  override def check(repeatedField: Type): Boolean = {
    if (repeatedField.isPrimitive || !(repeatedField.getName == "list")) {
      false
    } else {
      elementType(repeatedField).getName == "element"
    }
  }

  override def elementType(repeatedType: Type): Type = firstField(repeatedType.asGroupType)

  override private[scrooge] def isElementRequired(repeatedType: Type): Boolean = elementType(repeatedType).getRepetition eq Type.Repetition.REQUIRED

  override def elementName(repeatedType: Type): String = "element"

  override def createCompliantRepeatedType(typ: Type, name: String, isElementRequired: Boolean, originalType: OriginalType): Type = {

    val repetition = if (isElementRequired) Type.Repetition.REQUIRED else Type.Repetition.OPTIONAL
    val elementType = if (typ.isPrimitive) {

      new PrimitiveType(repetition, typ.asPrimitiveType.getPrimitiveTypeName, "element", originalType)
    } else {
      val listType = if (ParquetListFormatRule.isGroupList(typ)) OriginalType.LIST else null
      new GroupType(
        repetition,
        "element",
        listType,
        // TODO: generalize this
        // we cannot flatten `list`
        if (typ.asGroupType.getName == "list") util.Arrays.asList(typ) else typ.asGroupType.getFields)
    }

    new GroupType(Type.Repetition.REPEATED, "list", util.Arrays.asList(elementType))
  }

  private def firstField(groupType: GroupType): Type = {
    groupType.getFields.get(0)
  }
}
