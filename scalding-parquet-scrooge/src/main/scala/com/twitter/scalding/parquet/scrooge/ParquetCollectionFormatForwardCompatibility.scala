package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.slf4j.LoggerFactory

private[scrooge] case class GroupUnwrapped(wrappers: Seq[GroupType], repeatedType: Type)

object ParquetCollectionFormatForwardCompatibility {

  private val LOGGER = LoggerFactory.getLogger(getClass)

  private val SOURCE_LIST_RULES = List(
    PrimitiveElementRule, PrimitiveArrayRule,
    GroupElementRule, GroupArrayRule,
    TupleRule, StandardRule
  )

  private val TARGET_LIST_RULES = SOURCE_LIST_RULES.filterNot(_ == TupleRule)

  /**
   * Create a forward-compatible schema, using content from source type with format from target type.
   * @param sourceType source type with legacy format
   * @param targetType target type to which source is converted to
   */
  def forwardCompatibleMessage(sourceType: MessageType, targetType: MessageType): MessageType = {
    val groupResult = forwardCompatibleType(sourceType, targetType).asGroupType()
    LOGGER.info("Making source schema to be compatible with target" +
      s"\nSource:\n${sourceType}\nTarget:\n${targetType}\nResult:\n${groupResult}")
    new MessageType(groupResult.getName, groupResult.getFields)
  }

  private def wrapElementAsRepeatedType(rule: ParquetListFormatRule, repeatedType: Type, elementType: Type): Type = {
    rule.createCompliantRepeatedType(
      elementType,
      rule.elementName(repeatedType),
      // if repeated or required, it is required
      !elementType.isRepetition(Type.Repetition.OPTIONAL), elementType.getOriginalType)
  }

  private def forwardCompatibleType(sourceType: Type, targetType: Type): Type = {
    if (sourceType.isPrimitive || targetType.isPrimitive) {
      return sourceType
    }

    val sourceUnwrapped = unwrapGroup(sourceType.asGroupType)
    val targetUnwrapped = unwrapGroup(targetType.asGroupType)

    if (targetUnwrapped.isDefined && sourceUnwrapped.isDefined) {
      val targetRepeated = targetUnwrapped.get.repeatedType
      val sourceRepeated = sourceUnwrapped.get.repeatedType

      val sourceRule = findFirstRule(SOURCE_LIST_RULES, sourceRepeated, "source")
      val targetRule = findFirstRule(TARGET_LIST_RULES, targetRepeated, "target")

      val repeatedFormatted = (sourceRule, targetRule) match {
        case (Some(sRule), Some(tRule)) => {
          val sourceElement = sRule.elementType(sourceRepeated)
          val targetElement = tRule.elementType(targetRepeated)
          // Recurse on the element. This is to handle nested list
          val forwardCompatElement = forwardCompatibleType(sourceElement, targetElement)
          // Wrap the solved element with current source structure, and do actual conversion work
          val repeatedResolved = wrapElementAsRepeatedType(sRule, sourceRepeated, forwardCompatElement)
          convertToTarget(targetRepeated, repeatedResolved)
        }
        case _ => sourceRepeated // No-op
      }

      // Wrapped the solved repeated type in its original groups,
      // describing field name and whether it's optional/required
      sourceUnwrapped.get.wrappers.foldRight(repeatedFormatted) {
        (wrapper, group) => wrapper.withNewFields(group)
      }
    } else {
      val sourceGroup = sourceType.asGroupType
      val targetGroup = targetType.asGroupType

      val resultFields = new util.ArrayList[Type]
      import scala.collection.JavaConversions._
      for (sourceField <- sourceGroup.getFields) {
        if (!targetGroup.containsField(sourceField.getName)) { // This can happen when
          // 1) projecting optional field over non-existent target schema
          // 2) field is a part of legacy map format
          // Make no assertions (separation of responsibility) and just include it
          resultFields.add(sourceField)
        }
        else {
          val fieldIndex = targetGroup.getFieldIndex(sourceField.getName)
          val targetField = targetGroup.getFields.get(fieldIndex)
          resultFields.add(forwardCompatibleType(sourceField, targetField))
        }
      }
      sourceGroup.withNewFields(resultFields)
    }
  }

  private def findFirstRule(rules: Seq[ParquetListFormatRule],
                            repeatedType: Type,
                            debuggingTypeSource: String): Option[ParquetListFormatRule] = {
    val ruleFound = rules.find(rule => rule.check(repeatedType))
    if (ruleFound.isEmpty) {
      LOGGER.warn(s"Unable to find matching rule for $debuggingTypeSource schema:\n$repeatedType")
    }
    ruleFound
  }

  private def convertToTarget(repeatedTargetType: Type, repeatedSourceType: Type): Type = {
    val sourceRuleMaybe = findFirstRule(SOURCE_LIST_RULES, repeatedSourceType, "source")
    val targetRuleMaybe = findFirstRule(TARGET_LIST_RULES, repeatedTargetType, "target")
    if (sourceRuleMaybe == targetRuleMaybe || sourceRuleMaybe.isEmpty || targetRuleMaybe.isEmpty) {
      repeatedSourceType
    } else {
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

  private def unwrapGroup(typ: Type, wrappers: Seq[GroupType] = Seq()): Option[GroupUnwrapped] = {
    if (typ.isPrimitive || typ.asGroupType.getFieldCount != 1) {
      None
    } else {
      if (ParquetListFormatRule.isGroupList(typ)) { // when it is repeated
        Some(GroupUnwrapped(wrappers :+ typ.asGroupType(), typ.asGroupType.getFields.get(0)))
      } else {
        unwrapGroup(typ.asGroupType.getFields.get(0), wrappers :+ typ.asGroupType())
      }
    }
  }
}
