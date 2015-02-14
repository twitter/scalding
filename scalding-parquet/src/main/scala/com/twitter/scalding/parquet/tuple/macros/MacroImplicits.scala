package com.twitter.scalding.parquet.tuple.macros

import scala.language.experimental.macros

import com.twitter.scalding.parquet.tuple.macros.impl.SchemaProviderImpl
import _root_.parquet.schema.MessageType

object MacroImplicits {
  implicit def materializeCaseClassTypeDescriptor[T]: MessageType = macro SchemaProviderImpl.toParquetSchemaImp[T]
}
