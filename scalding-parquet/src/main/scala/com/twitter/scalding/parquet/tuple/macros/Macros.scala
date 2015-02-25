package com.twitter.scalding.parquet.tuple.macros

import com.twitter.scalding.parquet.tuple.macros.impl.{ ParquetSchemaProvider, ParquetTupleConverterProvider, WriteSupportProvider }
import com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType

import scala.language.experimental.macros

object Macros {
  /**
   * Macro used to generate parquet schema for a given case class that contains only primitive fields.
   * Option field and nested group is supported. For example if we have:
   *
   * case class SampleClassA(x: Int, y: String)
   * case class SampleClassB(a: SampleClassA, y: String)
   *
   * The macro will generate a parquet message type like this:
   *
   * """
   *   message SampleClassB {
   *     required group a {
   *       required int32 x;
   *       required binary y;
   *     }
   *     required binary y;
   *   }
   *  """
   *
   * @tparam T Case class type that contains only primitive fields or nested case class.
   * @return Generated case class parquet message type string
   */
  def caseClassParquetSchema[T]: String = macro ParquetSchemaProvider.toParquetSchemaImpl[T]

  /**
   * Macro used to generate parquet tuple converter for a given case class that contains only primitive fields.
   * Option field and nested group is supported.
   *
   * @tparam T Case class type that contains only primitive fields or nested case class.
   * @return Generated parquet converter
   */
  def caseClassParquetTupleConverter[T]: ParquetTupleConverter = macro ParquetTupleConverterProvider.toParquetTupleConverterImpl[T]

  /**
   * Macro used to generate case class write support to parquet.
   * @tparam T User defined case class tuple type.
   * @return Generated case class tuple write support function.
   */
  def caseClassWriteSupport[T]: (T, RecordConsumer, MessageType) => Unit = macro WriteSupportProvider.toWriteSupportImpl[T]
}
