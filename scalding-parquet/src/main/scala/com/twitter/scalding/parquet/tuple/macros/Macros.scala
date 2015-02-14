package com.twitter.scalding.parquet.tuple.macros

import scala.language.experimental.macros

import com.twitter.scalding.parquet.tuple.macros.impl.SchemaProviderImpl

object Macros {
  def caseClassParquetSchema[T]: _root_.parquet.schema.MessageType = macro SchemaProviderImpl.toParquetSchemaImp[T]
}
