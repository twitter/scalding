package com.twitter.scalding.parquet.tuple.macros

import com.twitter.scalding.parquet.tuple.macros.impl.{ ParquetSchemaProvider, ParquetTupleConverterProvider, WriteSupportProvider }
import com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType

import scala.language.experimental.macros

/**
 * Macros used to generate parquet tuple read/write support.
 * These macros support only case class that contains primitive fields or nested case classes and also collection fields
 * like scala List, Set, and Map.
 * @author Jian TANG
 */
object Macros {
  /**
   * Macro used to generate parquet schema for a given case class. For example if we have:
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
   * @tparam T Case class type that contains primitive fields or collection fields or nested case class.
   * @return Generated case class parquet message type string
   */
  def caseClassParquetSchema[T]: String = macro ParquetSchemaProvider.toParquetSchemaImpl[T]

  /**
   * Macro used to generate parquet tuple converter for a given case class.
   *
   * @tparam T Case class type that contains primitive or collection type fields or nested case class.
   * @return Generated parquet converter
   */
  def caseClassParquetTupleConverter[T]: ParquetTupleConverter[T] = macro ParquetTupleConverterProvider.toParquetTupleConverterImpl[T]

  /**
   * Macro used to generate case class write support to parquet.
   * @tparam T User defined case class tuple type.
   * @return Generated case class tuple write support function.
   */
  def caseClassWriteSupport[T]: (T, RecordConsumer, MessageType) => Unit = macro WriteSupportProvider.toWriteSupportImpl[T]
}
