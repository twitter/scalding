package org.apache.parquet.column

object EncodingAccessor {
  def getMaxLevel(encoding: Encoding, descriptor: ColumnDescriptor, valuesType: ValuesType) =
    encoding.getMaxLevel(descriptor, valuesType)
}
