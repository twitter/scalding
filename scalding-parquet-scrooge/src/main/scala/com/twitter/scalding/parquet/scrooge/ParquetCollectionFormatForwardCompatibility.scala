package com.twitter.scalding.parquet.scrooge

import scala.collection.JavaConverters._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.apache.parquet.thrift.DecodingSchemaMismatchException
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object ParquetCollectionFormatForwardCompatibility {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a forward-compatible schema, using content from source type with format from target type.
   *
   * @param sourceType source type with legacy format
   * @param targetType target type to which source is converted to
   */
  def formatForwardCompatibleMessage(sourceType: MessageType, targetType: MessageType): MessageType = {
    val groupResult = formatForwardCompatibleType(sourceType, targetType).asGroupType()
    logger.debug("Making source schema to be compatible with target" +
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
    (findCollectionGroup(sourceType), findCollectionGroup(targetType)) match {
      case (Some(sourceGroup: ListGroup), Some(targetGroup: ListGroup)) =>
        formatForwardCompatibleCollectionGroup[ListGroup](sourceGroup, targetGroup)
      case (Some(sourceGroup: MapGroup), Some(targetGroup: MapGroup)) =>
        formatForwardCompatibleCollectionGroup[MapGroup](sourceGroup, targetGroup)
      case _ if sourceType.isPrimitive || targetType.isPrimitive => // Base case
        sourceType
      case _ => // Field projection
        val sourceGroup = sourceType.asGroupType
        val targetGroup = targetType.asGroupType
        val resultFields = sourceGroup.getFields.asScala.map { sourceField =>
          if (!targetGroup.containsField(sourceField.getName)) {
            if (!sourceField.isRepetition(Repetition.OPTIONAL)) {
              throw new DecodingSchemaMismatchException(
                s"Found non-optional source field ${sourceField.getName}:\n$sourceField\n\n" +
                  s"not present in the given target type:\n${targetGroup}"
              )
            }
            sourceField
          }
          else {
            val fieldIndex = targetGroup.getFieldIndex(sourceField.getName)
            val targetField = targetGroup.getFields.get(fieldIndex)
            formatForwardCompatibleType(sourceField, targetField)
          }
        }
        sourceGroup.withNewFields(resultFields.asJava)
    }
  }

  private def formatForwardCompatibleCollectionGroup[T <: CollectionGroup](sourceGroup: T,
                                                                           targetGroup: T)
                                                                          (implicit t: ClassTag[T]): GroupType = {

    val formatter = t.runtimeClass.asInstanceOf[Class[T]] match {
      case c if c == classOf[MapGroup] => ParquetMapFormatter
      case c if c == classOf[ListGroup] => ParquetListFormatter
    }
    val formattedRepeated = formatter.formatForwardCompatibleRepeatedType(
      sourceGroup.repeatedType,
      targetGroup.repeatedType,
      formatForwardCompatibleType(_, _))
    // Wrapped the formatted repeated type in its original groups,
    // describing field name and whether it's optional/required
    sourceGroup.groupWrapper.withNewFields(formattedRepeated)
  }

  private def findCollectionGroup(typ: Type): Option[CollectionGroup] = {
    ParquetListFormatter.extractGroup(typ).orElse(ParquetMapFormatter.extractGroup(typ))
  }
}

private[scrooge] trait ParquetCollectionFormatter {
  def formatForwardCompatibleRepeatedType(sourceRepeatedMapType: Type,
                                          targetRepeatedMapType: Type,
                                          recursiveSolver: (Type, Type) => Type): Type

  def extractGroup(typ: Type): Option[CollectionGroup]
}

private[scrooge] sealed trait CollectionGroup {
  def groupWrapper: GroupType

  def repeatedType: Type
}

private[scrooge] sealed case class MapGroup(groupWrapper: GroupType, repeatedType: Type) extends CollectionGroup

private[scrooge] sealed case class ListGroup(groupWrapper: GroupType, repeatedType: Type) extends CollectionGroup