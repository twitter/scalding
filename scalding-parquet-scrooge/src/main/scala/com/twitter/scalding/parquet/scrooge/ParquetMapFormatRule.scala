package com.twitter.scalding.parquet.scrooge

import org.apache.parquet.schema.{OriginalType, Type}

/**
 * Rule to format parquet schema of legacy map type to standard target
 * with repeated type of `key_value` without annotation
 * as recommended in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
 *
 * Source with legacy format created by
 * {@code org.apache.parquet.schema.ConversionPatterns} has repeated `map` field
 * annotated with (MAP_KEY_VALUE)
 */
private[scrooge] object ParquetMapFormatRule extends ParquetCollectionFormatRule {

  /**
   * Handle legacy type when
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
   * @param sourceRepeatedMapType
   * @param targetRepeatedMapType
   * @return
   */
  def formatForwardCompatibleRepeatedType(sourceRepeatedMapType: Type, targetRepeatedMapType: Type) = {
    val isLegacyToStandardFormat = isStandardRepeatedType(targetRepeatedMapType) &&
      isLegacyRepeatedType(sourceRepeatedMapType)
    if (isLegacyToStandardFormat) {
      targetRepeatedMapType.asGroupType().withNewFields(sourceRepeatedMapType.asGroupType().getFields)
    } else {
      sourceRepeatedMapType
    }
  }

  def isGroupMap(typ: Type): Boolean = {
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
    ((repeatedType.getName == "map") && (repeatedType.getOriginalType == OriginalType.MAP_KEY_VALUE))
  }

  private def isStandardRepeatedType(repeatedType: Type) = {
    (repeatedType.getName == "key_value") && (repeatedType.getOriginalType == null)
  }
}
