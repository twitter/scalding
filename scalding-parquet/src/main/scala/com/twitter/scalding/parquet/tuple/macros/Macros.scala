package com.twitter.scalding.parquet.tuple.macros

import com.twitter.scalding.parquet.tuple.macros.impl.{ ParquetReadSupportProvider, ParquetSchemaProvider, WriteSupportProvider }
import com.twitter.scalding.parquet.tuple.scheme.{ ParquetReadSupport, ParquetWriteSupport }

import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros
import macrocompat.bundle

/**
 * Macros used to generate parquet tuple read/write support.
 * These macros support only case class that contains primitive fields or nested case classes and also collection fields
 * like scala List, Set, and Map.
 * @author Jian TANG
 */

@bundle
class Impl(val c: Context) {
  val schemaProvider = new ParquetSchemaProvider((s: String) => s)
  val r = new ParquetReadSupportProvider(schemaProvider)
  val w = new WriteSupportProvider(schemaProvider)

  def schema[T: c.WeakTypeTag]: c.Expr[String] =
    schemaProvider.toParquetSchemaImpl[T](c)

  def readSupport[T: c.WeakTypeTag]: c.Expr[ParquetReadSupport[T]] =
    r.toParquetReadSupportImpl[T](c)

  def writeSupport[T: c.WeakTypeTag]: c.Expr[ParquetWriteSupport[T]] =
    w.toWriteSupportImpl[T](c)
}

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
  implicit def caseClassParquetSchema[T]: String = macro Impl.schema[T]

  /**
   * Macro generated case class read support
   */
  implicit def caseClassParquetReadSupport[T]: ParquetReadSupport[T] = macro Impl.readSupport[T]

  /**
   * Macro used to generate case class write support to parquet.
   * @tparam T User defined case class tuple type.
   * @return Generated case class tuple write support function.
   */
  implicit def caseClassParquetWriteSupport[T]: ParquetWriteSupport[T] = macro Impl.writeSupport[T]
}
