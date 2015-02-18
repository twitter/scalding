package com.twitter.scalding.parquet.tuple.macros

import scala.language.experimental.macros

import com.twitter.scalding.parquet.tuple.macros.impl.SchemaProviderImpl
import _root_.parquet.schema.MessageType

object MacroImplicits {
  //implicits conversion from case class to parquet message type
  implicit def materializeCaseClassParquetSchema[T]: MessageType = macro SchemaProviderImpl.toParquetSchemaImp[T]
}
