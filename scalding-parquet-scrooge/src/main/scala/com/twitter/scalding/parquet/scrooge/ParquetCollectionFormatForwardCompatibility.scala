package com.twitter.scalding.parquet.scrooge

import java.util

import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.apache.parquet.thrift.DecodingSchemaMismatchException
import org.slf4j.LoggerFactory

object ParquetCollectionFormatForwardCompatibility {

  private val LOGGER = LoggerFactory.getLogger(getClass)

  /**
   * Create a forward-compatible schema, using content from source type with format from target type.
   * @param sourceType source type with legacy format
   * @param targetType target type to which source is converted to
   */
  def formatForwardCompatibleMessage(sourceType: MessageType, targetType: MessageType): MessageType = {
    val groupResult = formatForwardCompatibleType(sourceType, targetType).asGroupType()
    LOGGER.info("Making source schema to be compatible with target" +
      s"\nSource:\n${sourceType}\nTarget:\n${targetType}\nResult:\n${groupResult}")
    new MessageType(groupResult.getName, groupResult.getFields)
  }

  /**
   * Traverse source/target schemas and format nodes of list or map.
   * The formatting is not to one-to-one node swapping from source to target,
   * this is because the subset fields of source node and its optional/required must
   * be maintained in the formatted result.
   */
  private def formatForwardCompatibleType(sourceType: Type, targetType: Type): Type = {
    (unwrapGroup(sourceType), unwrapGroup(targetType)) match {
      case _ if sourceType.isPrimitive || targetType.isPrimitive =>
        // Base case
        sourceType
      case (
        GroupUnwrapped(sourceWrappers, Some(sourceRepeatedListType), None),
        GroupUnwrapped(_, Some(targetRepeatedListType), None)
      ) =>
        // Format list
        val sourceRule = ParquetListFormatRule.findFirstListRule(sourceRepeatedListType, Source)
        val targetRule = ParquetListFormatRule.findFirstListRule(targetRepeatedListType, Target)

        val formattedRepeated = (sourceRule, targetRule) match {
          case (Some(sourceRule), Some(targetRule)) => {
            val sourceElement = sourceRule.elementType(sourceRepeatedListType)
            val targetElement = targetRule.elementType(targetRepeatedListType)
            // Recurse on the element instead of `repeated` type because list still can have
            // different formats at repeated type
            val forwardCompatElement = formatForwardCompatibleType(sourceElement, targetElement)
            // Wrap the solved element with current source structure, and do actual conversion work
            val forwardCompatRepeated = ParquetListFormatRule.wrapElementAsRepeatedType(
              sourceRule,
              sourceRepeatedListType,
              forwardCompatElement
            )
            ParquetListFormatRule.formatForwardCompatibleRepeatedType(
              forwardCompatRepeated,
              targetRepeatedListType)
          }
          case _ => sourceRepeatedListType // No-op
        }
        // Wrapped the formatted repeated type in its original groups,
        // describing field name and whether it's optional/required
        sourceWrappers.foldRight(formattedRepeated) {
          (wrapper, group) => wrapper.withNewFields(group)
        }
      case (
        GroupUnwrapped(sourceWrappers, None, Some(sourceRepeatedMapType)),
        GroupUnwrapped(_, None, Some(targetRepeatedMapType))
      ) =>
        // Format map
        val forwardCompatRepeated = formatForwardCompatibleType(sourceRepeatedMapType, targetRepeatedMapType)
        val formattedRepeated = ParquetMapFormatRule.formatForwardCompatibleRepeatedType(
          forwardCompatRepeated,
          targetRepeatedMapType
        )
        // Wrapped the formatted repeated type in its original groups,
        // describing field name and whether it's optional/required
        sourceWrappers.foldRight(formattedRepeated) {
          (wrapper, group) => wrapper.withNewFields(group)
        }
      case _ =>
        // Field projection
        val sourceGroup = sourceType.asGroupType
        val targetGroup = targetType.asGroupType

        val resultFields = new util.ArrayList[Type]
        import scala.collection.JavaConversions._
        for (sourceField <- sourceGroup.getFields) {
          if (!targetGroup.containsField(sourceField.getName)) {
            if (!sourceField.isRepetition(Repetition.OPTIONAL)) {
              throw new DecodingSchemaMismatchException(
                s"Found non-optional source field ${sourceField.getName}:\n$sourceField\n\n" +
                  s"not present in the given target type:\n${targetGroup}"
              )
            }
            resultFields.add(sourceField)
          }
          else {
            val fieldIndex = targetGroup.getFieldIndex(sourceField.getName)
            val targetField = targetGroup.getFields.get(fieldIndex)
            resultFields.add(formatForwardCompatibleType(sourceField, targetField))
          }
        }
        sourceGroup.withNewFields(resultFields)
    }
  }

  private case class GroupUnwrapped(wrappers: Seq[GroupType],
                                    repeatedListType: Option[Type] = None,
                                    repeatedMapType: Option[Type] = None)

  private def unwrapGroup(typ: Type, wrappers: Seq[GroupType] = Seq()): GroupUnwrapped = {
    if (typ.isPrimitive) {
      GroupUnwrapped(
        wrappers,
        repeatedListType=None,
        repeatedMapType=None
      )
    } else if (typ.asGroupType.getFieldCount != 1) {
      GroupUnwrapped(
        wrappers :+ typ.asGroupType(),
        repeatedListType=None,
        repeatedMapType=None
      )
    } else {
      // Note the field count is strictly 1 here, and the wrappers will be used later
      // to wrap back the formatted results.
      if (ParquetListFormatRule.isGroupList(typ)) {
        GroupUnwrapped(
          wrappers :+ typ.asGroupType(),
          repeatedListType=Some(typ.asGroupType.getFields.get(0)),
          repeatedMapType = None
        )
      } else if (ParquetMapFormatRule.isGroupMap(typ)) {
        GroupUnwrapped(
          wrappers :+ typ.asGroupType(),
          repeatedListType=None,
          repeatedMapType=Some(typ.asGroupType.getFields.get(0))
        )
      } else {
        unwrapGroup(typ.asGroupType.getFields.get(0), wrappers :+ typ.asGroupType())
      }
    }
  }
}

trait ParquetCollectionFormatRule {
  def formatForwardCompatibleRepeatedType(sourceRepeatedMapType: Type, targetRepeatedMapType: Type): Type
}