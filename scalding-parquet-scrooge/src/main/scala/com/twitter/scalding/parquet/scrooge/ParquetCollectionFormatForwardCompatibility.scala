package com.twitter.scalding.parquet.scrooge

import scala.collection.JavaConverters._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.apache.parquet.thrift.DecodingSchemaMismatchException
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Project file schema to have collection types--list and map--in the same structure
 * as projected read schema. This is currently used in [[ScroogeReadSupport]] where projected
 * read schema can come from:
 * 1) Thrift struct via [[org.apache.parquet.thrift.ThriftSchemaConvertVisitor]] which always
 * describe list with `_tuple` format, and map which has `MAP_KEY_VALUE` annotation.
 * 2) User-supplied schema string via config key
 * [[org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA]]
 *
 * The strategy of this class is to first assume that the projected read schema is a "sub-graph" of
 * file schema in terms of field names. (We allow optional field in projected read schema to be in
 * the projected file schema.) However, the data types for collection can differ in
 * graph structure between the two schemas.
 * We thus need to:
 * 1) traverse the two schemas until we find the collection type indicated by `repeated` type.
 * 2) delegate the collection types found to respective list/map formatter.
 */
private[scrooge] object ParquetCollectionFormatForwardCompatibility {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Project file schema to contain the same fields as the given projected read schema.
   * The result projected file schema should have the same optional/required fields as the
   * projected read schema, but maintain collection type format for the file schema.
   *
   * @param projectedReadSchema read schema specifying field projection
   * @param fileSchema file schema to be projected
   */
  def projectFileSchema(projectedReadSchema: MessageType, fileSchema: MessageType): MessageType = {
    val projectedFileSchema = projectFileType(projectedReadSchema, fileSchema, FieldContext()).asGroupType()
    logger.debug(s"Projected read schema:\n${projectedReadSchema}\n" +
      s"File schema:\n${fileSchema}\n" +
      s"Projected file schema:\n${projectedFileSchema}")
    new MessageType(projectedFileSchema.getName, projectedFileSchema.getFields)
  }

  /**
   * Traverse given schemas and format node for list or map of projected read type to structure
   * of file schema. The formatting is not to one-to-one node swapping between the two schemas
   * because of the projection requirement.
   */
  private def projectFileType(projectedReadType: Type, fileType: Type, fieldContext: FieldContext): Type = {
    (extractCollectionGroup(projectedReadType), extractCollectionGroup(fileType)) match {
      case _ if projectedReadType.isPrimitive && fileType.isPrimitive =>
        projectedReadType
      case _ if projectedReadType.isPrimitive != fileType.isPrimitive =>
        throw new DecodingSchemaMismatchException(
          s"Found schema mismatch between projected read type:\n$projectedReadType\n" +
            s"and file type:\n${fileType}"
        )
      case (Some(projectedReadGroup: ListGroup), Some(fileGroup: ListGroup)) =>
        projectFileGroup[ListGroup](projectedReadGroup, fileGroup, fieldContext)
      case (Some(projectedReadGroup: MapGroup), Some(fileGroup: MapGroup)) =>
        projectFileGroup[MapGroup](projectedReadGroup, fileGroup, fieldContext)
      case _ => // Field projection
        val projectedReadGroupType = projectedReadType.asGroupType
        val fileGroupType = fileType.asGroupType
        val projectedReadFields = projectedReadGroupType.getFields.asScala.map { projectedReadField =>
          if (!fileGroupType.containsField(projectedReadField.getName)) {
            if (!projectedReadField.isRepetition(Repetition.OPTIONAL)) {
              throw new DecodingSchemaMismatchException(
                s"Found non-optional projected read field ${projectedReadField.getName}:\n$projectedReadField\n\n" +
                  s"not present in the given file group type:\n${fileGroupType}"
              )
            }
            projectedReadField
          }
          else {
            val fileFieldIndex = fileGroupType.getFieldIndex(projectedReadField.getName)
            val fileField = fileGroupType.getFields.get(fileFieldIndex)
            projectFileType(projectedReadField, fileField, FieldContext(projectedReadField.getName))
          }
        }
        projectedReadGroupType.withNewFields(projectedReadFields.asJava)
    }
  }

  private def projectFileGroup[T <: CollectionGroup](projectedReadGroup: T,
                                                     fileGroup: T,
                                                     fieldContext: FieldContext)(implicit t: ClassTag[T]): GroupType = {

    val (formatter, updatedFieldContext) = t.runtimeClass.asInstanceOf[Class[T]] match {
      case c if c == classOf[MapGroup] =>
        (ParquetMapFormatter, fieldContext.copy(nestedListLevel = fieldContext.nestedListLevel + 1))
      case c if c == classOf[ListGroup] =>
        (ParquetListFormatter, fieldContext.copy(nestedListLevel = fieldContext.nestedListLevel + 1))
    }

    val projectedFileRepeatedType = formatter.formatForwardCompatibleRepeatedType(
      projectedReadGroup.repeatedType,
      fileGroup.repeatedType,
      updatedFieldContext,
      projectFileType(_, _, _))
    // Respect optional/required from the projected read group.
    projectedReadGroup.groupType.withNewFields(projectedFileRepeatedType)
  }

  private def extractCollectionGroup(typ: Type): Option[CollectionGroup] = {
    ParquetListFormatter.extractGroup(typ).orElse(ParquetMapFormatter.extractGroup(typ))
  }
}

private[scrooge] trait ParquetCollectionFormatter {
  /**
   * Format source repeated type in the structure of target repeated type.
   * @param sourceRepeatedType repeated type from which the formatted result get content
   * @param targetRepeatedType repeated type from which the formatted result get the structure
   * @param recursiveSolver solver for the inner content of the repeated type
   * @return formatted result
   */
  def formatForwardCompatibleRepeatedType(sourceRepeatedType: Type,
                                          targetRepeatedType: Type,
                                          fieldContext: FieldContext,
                                          recursiveSolver: (Type, Type, FieldContext) => Type): Type

  /**
   * Extract collection group containing repeated type of different formats.
   */
  def extractGroup(typ: Type): Option[CollectionGroup]
}

/**
 * Helper class to carry information from the field. Currently it only contains specific to list collection
 * @param name      field name
 * @param nestedListLevel li
 */
private[scrooge] case class FieldContext(name: String="", nestedListLevel: Int=0)

private[scrooge] sealed trait CollectionGroup {
  /**
   * Type for the collection.
   * For example, given the schema,
   * required group my_list (LIST) {
   *   repeated group list {
   *     optional binary element (UTF8);
   *   }
   * }
   * [[groupType]] refers to this whole schema
   * [[repeatedType]] refers to inner `repeated` schema
   */
  def groupType: GroupType

  def repeatedType: Type
}

private[scrooge] sealed case class MapGroup(groupType: GroupType, repeatedType: Type) extends CollectionGroup

private[scrooge] sealed case class ListGroup(groupType: GroupType, repeatedType: Type) extends CollectionGroup