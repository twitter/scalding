package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.{GroupType, OriginalType, PrimitiveType, Type}
import org.slf4j.LoggerFactory

/**
 * Rule to convert parquet schema of legacy list type to standard one
 * namely 3-level list structure as recommended in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
 *
 * More specifically this handles converting from parquet file created by
 * {{@code org.apache.parquet.thrift.ThriftSchemaConvertVisitor}} which always suffix
 * list element with "_tuple".
 */
private[scrooge] object ParquetListFormatRule extends ParquetCollectionFormatRule {

  private val LOGGER = LoggerFactory.getLogger(getClass)

  def formatForwardCompatibleRepeatedType(repeatedSourceType: Type, repeatedTargetType: Type) = {
    val sourceRuleMaybe = findFirstListRule(repeatedSourceType, Source)
    val targetRuleMaybe = findFirstListRule(repeatedTargetType, Target)
    if (sourceRuleMaybe == targetRuleMaybe || sourceRuleMaybe.isEmpty || targetRuleMaybe.isEmpty) repeatedSourceType else {
      val sourceRule = sourceRuleMaybe.get
      val targetRule = targetRuleMaybe.get
      val elementType = sourceRule.elementType(repeatedSourceType)
      targetRule.createCompliantRepeatedType(
        typ= elementType,
        name= elementType.getName,
        isElementRequired= sourceRule.isElementRequired(repeatedSourceType),
        originalType= sourceRule.elementOriginalType(repeatedSourceType)
      )
    }
  }

  def isGroupList(projection: Type): Boolean = {
    if (projection.isPrimitive) return false
    val groupProjection = projection.asGroupType
    (groupProjection.getOriginalType eq OriginalType.LIST) && groupProjection.getFieldCount == 1 && groupProjection.getFields.get(0).isRepetition(Type.Repetition.REPEATED)
  }

  def findFirstListRule(repeatedType: Type,
                        sourceOrTarget: SourceOrTarget): Option[ParquetListFormatRule] = {
    val ruleFound = sourceOrTarget.rules.find(rule => rule.check(repeatedType))
    if (ruleFound.isEmpty) LOGGER.warn(s"Unable to find matching rule for ${sourceOrTarget.name} schema:\n$repeatedType")
    ruleFound
  }

  def wrapElementAsRepeatedType(rule: ParquetListFormatRule, repeatedType: Type, elementType: Type): Type = rule.createCompliantRepeatedType(
    elementType,
    rule.elementName(repeatedType),
    // if repeated or required, it is required
    !elementType.isRepetition(Type.Repetition.OPTIONAL),
    elementType.getOriginalType)
}

/**
 * Helper to specify supported source/target conversion.
 */
private[scrooge] sealed trait SourceOrTarget {
  def rules: Seq[ParquetListFormatRule]
  def name: String
}

private[scrooge] object Source extends SourceOrTarget {
  override val rules: Seq[ParquetListFormatRule] = Seq(
    PrimitiveElementRule, PrimitiveArrayRule,
    GroupElementRule, GroupArrayRule,
    TupleRule, StandardRule
  )

  override def name: String = "source"
}

private[scrooge] object Target extends SourceOrTarget {
  override def rules: Seq[ParquetListFormatRule] = Source.rules.filterNot(_ == TupleRule)

  override def name: String = "target"
}

/**
 * Rule allowing conversion from one format to other format by
 * 1) detect which format is the repeated list type.
 * 2) decompose the repeated type into element and other info.
 * 3) construct compliant repeated type from the given element and other info.
 * For example,
 * if source repeated type matches Rule 1, and target type matches Rule 2.
 * Rule 1 will decompose the source type, and
 * Rule 2 will take that information to construct repeated element in Rule 2 format.
 */
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


private[scrooge] sealed trait PrimitiveListRule extends ParquetListFormatRule {
  /**
   * repeated int32 [element|array];
   */
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

private[scrooge] object PrimitiveElementRule extends PrimitiveListRule {
  override def constantElementName: String = "element"
}

private[scrooge] object PrimitiveArrayRule extends PrimitiveListRule {
  override def constantElementName: String = "array"
}

private[scrooge] sealed trait GroupListRule extends ParquetListFormatRule {
  /**
   * repeated group [element|array] {
   *   required binary str (UTF8);
   *   required int32 num;
   * }
   */
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

private[scrooge] object GroupElementRule extends GroupListRule {
  override def constantElementName: String = "element"
}

private[scrooge] object GroupArrayRule extends GroupListRule {
  override def constantElementName: String = "array"
}

private[scrooge] object TupleRule extends ParquetListFormatRule {
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
    else new GroupType(Type.Repetition.REPEATED, suffixed_name, originalType, typ.asGroupType.getFields)
  }
}

private[scrooge] object StandardRule extends ParquetListFormatRule {
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

  override def createCompliantRepeatedType(originalElementType: Type, name: String, isElementRequired: Boolean, originalType: OriginalType): Type = {

    val repetition = if (isElementRequired) Type.Repetition.REQUIRED else Type.Repetition.OPTIONAL
    val elementType = if (originalElementType.isPrimitive) {
      new PrimitiveType(repetition, originalElementType.asPrimitiveType.getPrimitiveTypeName, "element", originalType)
    } else {
      new GroupType(
        repetition,
        "element",
        originalType,
        originalElementType.asGroupType.getFields)
    }

    new GroupType(Type.Repetition.REPEATED, "list", util.Arrays.asList(elementType))
  }

  private def firstField(groupType: GroupType): Type = {
    groupType.getFields.get(0)
  }
}
