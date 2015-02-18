package com.twitter.scalding.parquet.tuple.macros

import parquet.schema.MessageType

import scala.language.experimental.macros

import com.twitter.scalding.parquet.tuple.macros.impl.SchemaProviderImpl

object Macros {
  def caseClassParquetSchema[T]: MessageType = macro SchemaProviderImpl.toParquetSchemaImp[T]
}
