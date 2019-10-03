package com.twitter.scalding.parquet.scrooge

import org.apache.parquet.schema.{OriginalType, Type}

/**
 * Format parquet schema of legacy map type to standard target
 * with repeated type of `key_value` without annotation
 * as recommended in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
 *
 * Source with legacy format created by
 * [[org.apache.parquet.schema.ConversionPatterns]] has repeated `map` field
 * annotated with (MAP_KEY_VALUE)
 */
private[scrooge] object ParquetMapFormatter extends ParquetCollectionFormatter {

  /**
   * Handle map format compatibility
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
   *
   * @param sourceRepeatedMapType
   * @param targetRepeatedMapType
   */
  def formatForwardCompatibleRepeatedType(sourceRepeatedMapType: Type,
                                          targetRepeatedMapType: Type,
                                          fieldContext: FieldContext,
                                          recursiveSolver: (Type, Type, FieldContext) => Type) = {

    val solvedRepeatedType = recursiveSolver(sourceRepeatedMapType, targetRepeatedMapType, fieldContext)
    targetRepeatedMapType.asGroupType().withNewFields(solvedRepeatedType.asGroupType().getFields)
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
