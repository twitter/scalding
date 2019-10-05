package com.twitter.scalding.parquet.scrooge

import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.apache.parquet.thrift.DecodingSchemaMismatchException
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Project file schema based on projected read schema which may contain different format
 * of collection group--list and map. This is currently used in [[ScroogeReadSupport]] where
 * projected read schema can come from:
 * 1) Thrift struct via [[org.apache.parquet.thrift.ThriftSchemaConvertVisitor]] which always
 * describe list with `_tuple` format, and map which has `MAP_KEY_VALUE` annotation.
 * 2) User-supplied schema string via config key
 * [[org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA]]
 *
 * By definition, the projected read schema is a "sub-graph" of file schema in terms of field names.
 * (We do allow optional field in projected read schema to be in
 * the projected file schema, even if file schema may not originally contain it.)
 * The graphs of the two schemas may, however, differ for list and map type because of multiple
 * legacy formats and the canonical one. This class supports all directions of conversion.
 *
 * The projection strategy is:
 * 1) traverse the two schemas and maintain only the fields in the read schema.
 * 2) find collection type indicated by `repeated` type, and delegate it to respective list/map formatter.
 * 3) wrap back the formatted repeated type with group type from projected read schema. This
 * means the optional/required remains the same as that from projected read schema.
 */
private[scrooge] object ParquetCollectionFormatCompatibility {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Project file schema to contain the same fields as the given projected read schema.
   * The result is projected file schema with the same optional/required fields as the
   * projected read schema, but collection type format as the file schema.
   *
   * @param fileSchema file schema to be projected
   * @param projectedReadSchema read schema specifying field projection
   */
  def projectFileSchema(fileSchema: MessageType, projectedReadSchema: MessageType): MessageType = {
    val projectedFileSchema = projectFileType(fileSchema, projectedReadSchema, FieldContext()).asGroupType()
    logger.debug(s"Projected read schema:\n${projectedReadSchema}\n" +
      s"File schema:\n${fileSchema}\n" +
      s"Projected file schema:\n${projectedFileSchema}")
    new MessageType(projectedFileSchema.getName, projectedFileSchema.getFields)
  }

  /**
   * Main recursion to get projected file type. Traverse given schemas, filter out unneeded
   * fields, and format read schema's list/map node to file schema's structure.
   * The formatting of repeated type is not to one-to-one node swapping because we also have to
   * handle projection and possible nested collection types in the repeated type.
   */
  private def projectFileType(fileType: Type, projectedReadType: Type, fieldContext: FieldContext): Type = {
    if (projectedReadType.isPrimitive || fileType.isPrimitive) {
      // Base-cases to handle primitive types:
      if (projectedReadType.isPrimitive && fileType.isPrimitive) {
        // The field is a primitive in both schemas
        projectedReadType
      } else {
        // The field is primitive in one schema but non-primitive in the othe other
        throw new DecodingSchemaMismatchException(
          s"Found schema mismatch between projected read type:\n$projectedReadType\n" +
            s"and file type:\n${fileType}"
        )
      }
    } else {
      // Recursive cases to handle non-primitives (lists, maps, and structs):
      (extractCollectionGroup(projectedReadType), extractCollectionGroup(fileType)) match {
        case (Some(projectedReadGroup: ListGroup), Some(fileGroup: ListGroup)) =>
          projectFileGroup(fileGroup, projectedReadGroup, fieldContext.copy(nestedListLevel = fieldContext.nestedListLevel + 1), formatter=ParquetListFormatter)
        case (Some(projectedReadGroup: MapGroup), Some(fileGroup: MapGroup)) =>
          projectFileGroup(fileGroup, projectedReadGroup, fieldContext, formatter=ParquetMapFormatter)
        case _ => // Struct projection
          val projectedReadGroupType = projectedReadType.asGroupType
          val fileGroupType = fileType.asGroupType
          val projectedReadFields = projectedReadGroupType.getFields.asScala.map { projectedReadField =>
            if (!fileGroupType.containsField(projectedReadField.getName)) {
              // The projected read schema includes a field which is missing from the file schema.
              if (projectedReadField.isRepetition(Repetition.OPTIONAL)) {
                // The missing field is optional in the projected read schema. Since the file schema
                // doesn't contain this field there are no collection compatibility concerns to worry
                // about and we can simply use the supplied schema:
                projectedReadField
              } else {
                // The missing field is repeated or required, which is an error:
                throw new DecodingSchemaMismatchException(
                  s"Found non-optional projected read field ${projectedReadField.getName}:\n$projectedReadField\n\n" +
                    s"not present in the given file group type:\n${fileGroupType}"
                )
              }
            } else {
              // The field is present in both schemas, so first check that the schemas specify compatible repetition
              // values for the field, then recursively process the fields:
              val fileFieldIndex = fileGroupType.getFieldIndex(projectedReadField.getName)
              val fileField = fileGroupType.getFields.get(fileFieldIndex)
              if (fileField.isRepetition(Repetition.OPTIONAL) && projectedReadField.isRepetition(Repetition.REQUIRED)) {
                // The field is optional in the file schema but required in the projected read schema; this is an error:
                throw new DecodingSchemaMismatchException(
                  s"Found required projected read field ${projectedReadField.getName}:\n$projectedReadField\n\n" +
                    s"on optional file field:\n${fileField}"
                )
              } else {
                // The field's repetitions are compatible in both schemas (e.g. optional in both schemas or required
                // in both), so recursively process the field:
                projectFileType(fileField, projectedReadField, FieldContext(projectedReadField.getName))
              }
            }
          }
          projectedReadGroupType.withNewFields(projectedReadFields.asJava)
      }
    }
  }

  private def projectFileGroup(fileGroup: CollectionGroup,
                               projectedReadGroup: CollectionGroup,
                               fieldContext: FieldContext,
                               formatter: ParquetCollectionFormatter): GroupType = {
    val projectedFileRepeatedType = formatter.formatCompatibleRepeatedType(
      fileGroup.repeatedType,
      projectedReadGroup.repeatedType,
      fieldContext,
      projectFileType
    )
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
   *
   * @param fileRepeatedType repeated type from which the formatted result get the structure
   * @param readRepeatedType repeated type from which the formatted result get content
   * @param recursiveSolver solver for the inner content of the repeated type
   * @return formatted result
   */
  def formatCompatibleRepeatedType(fileRepeatedType: Type,
                                   readRepeatedType: Type,
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