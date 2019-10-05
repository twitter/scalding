package com.twitter.scalding.parquet.scrooge

import org.apache.parquet.schema.{OriginalType, Type}

/**
 * Format parquet map schema of read type to structure of file type.
 * The supported formats are:
 * 1) Standard repeated type of `key_value` without annotation
 * 2) Legacy repeated `map` field annotated with (MAP_KEY_VALUE)
 * as described in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
 *
 * In a common use case, read schema from thrift struct has legacy format 2) created by
 * [[org.apache.parquet.schema.ConversionPatterns]]
 */
private[scrooge] object ParquetMapFormatter extends ParquetCollectionFormatter {

  def formatCompatibleRepeatedType(fileRepeatedMapType: Type,
                                   readRepeatedMapType: Type,
                                   fieldContext: FieldContext,
                                   recursiveSolver: (Type, Type, FieldContext) => Type): Type = {
    val solvedRepeatedType = recursiveSolver(fileRepeatedMapType, readRepeatedMapType, fieldContext)
    fileRepeatedMapType.asGroupType().withNewFields(solvedRepeatedType.asGroupType().getFields)
  }

  def extractGroup(typ: Type): Option[MapGroup] = {
    if (isMapGroup(typ)) {
      Some(MapGroup(typ.asGroupType(), typ.asGroupType().getFields.get(0)))
    } else {
      None
    }
  }

  private def isMapGroup(typ: Type): Boolean = {
    if (typ.isPrimitive) {
      false
    } else {
      val groupType = typ.asGroupType
      (groupType.getOriginalType == OriginalType.MAP) &&
        (groupType.getFieldCount == 1) &&
        groupType.getFields.get(0).isRepetition(Type.Repetition.REPEATED) &&
        (isLegacyRepeatedType(groupType.getFields.get(0)) ||
          isStandardRepeatedType(groupType.getFields.get(0)))
    }
  }

  private def isLegacyRepeatedType(repeatedType: Type) = {
    (repeatedType.getName == "map") && (repeatedType.getOriginalType == OriginalType.MAP_KEY_VALUE)
  }

  private def isStandardRepeatedType(repeatedType: Type) = {
    (repeatedType.getName == "key_value") && (repeatedType.getOriginalType == null)
  }
}
