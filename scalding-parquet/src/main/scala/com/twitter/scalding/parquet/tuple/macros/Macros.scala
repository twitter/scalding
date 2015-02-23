package com.twitter.scalding.parquet.tuple.macros

import com.twitter.scalding.parquet.tuple.macros.impl.{ ParquetTupleConverterProvider, ParquetSchemaProvider, FieldValuesProvider }
import com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter

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
   * Macro used to generate function which permits to flat(at every level) a record to a index-value map.
   * For example if we have:
   *
   *   case class SampleClassA(short: Short, int: Int)
   *   case class SampleClassB(bool: Boolean, a: SampleClassA, long: Long, float: Float)
   *
   *   val b = SampleClassB(true, SampleClassA(1, 4), 6L, 5F)
   *
   * After flatting using the  generated function , we will get a map like this:
   *
   *     Map(0 -> true, 1 -> 1, 2 -> 4, 3 -> 6L, 4 -> 5F)
   * This macro can be used to define [[com.twitter.scalding.parquet.tuple.scheme.ParquetWriteSupport]].
   * See this class for more details.
   *
   * @tparam T Case class type that contains only primitive fields or nested case class.
   * @return Case class record field values flat function
   */
  def caseClassFieldValues[T]: T => Map[Int, Any] = macro FieldValuesProvider.toFieldValuesImpl[T]

  /**
   * Macro used to generate parquet tuple converter for a given case class that contains only primitive fields.
   * Option field and nested group is supported.
   *
   * @tparam T Case class type that contains only primitive fields or nested case class.
   * @return Generated parquet converter
   */
  def caseClassParquetTupleConverter[T]: ParquetTupleConverter = macro ParquetTupleConverterProvider.toParquetTupleConverterImpl[T]
}
