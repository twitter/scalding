package com.twitter.scalding.parquet.tuple

import _root_.parquet.filter2.predicate.FilterPredicate
import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.HasFilterPredicate
import com.twitter.scalding.parquet.tuple.scheme.{ ParquetReadSupport, ParquetWriteSupport, TypedParquetTupleScheme }

/**
 * Typed parquet tuple
 * @author Jian Tang
 */
object TypedParquet {
  /**
   * Create readable typed parquet source.
   * Here is an example:
   *  import com.twitter.scalding.parquet.tuple.macros.Macros._
   *  val parquetTuple = TypedParquet[SampleClass](Seq(outputPath),
   *    caseClassParquetWriteSupport[SampleClass](caseClassParquetSchema[SampleClass]))
   *
   * @param paths paths of parquet I/O
   * @tparam T Tuple type
   * @return a typed parquet source.
   */
  def apply[T](paths: Seq[String], readSupport: ParquetReadSupport[T]) =
    new TypedFixedPathParquetTuple[T](paths, readSupport, null)

  /**
   * Create readable typed parquet source with filter predicate.
   */
  def apply[T](paths: Seq[String], readSupport: ParquetReadSupport[T], fp: Option[FilterPredicate]) =
    new TypedFixedPathParquetTuple[T](paths, readSupport, null) {
      override def withFilter = fp
    }

  /**
   * Create typed parquet source supports both R/W.
   * @param paths paths of  parquet I/O
   * @tparam T Tuple type
   * @return a typed parquet source.
   */
  def apply[T](paths: Seq[String], readSupport: ParquetReadSupport[T], writeSupport: ParquetWriteSupport[T]) = {
    new TypedFixedPathParquetTuple[T](paths, readSupport, writeSupport)
  }

}

object TypedParquetSink {
  /**
   * Create typed parquet sink.
   * Here is an example:
   *  import com.twitter.scalding.parquet.tuple.macros.Macros._
   *  val sink = TypedParquetSink[SampleClass](Seq(outputPath),
   *    caseClassParquetWriteSupport[SampleClass](caseClassParquetSchema[SampleClass]))
   *
   * @param paths paths of parquet I/O
   * @tparam T Tuple type
   * @return a typed parquet source.
   */
  def apply[T](paths: Seq[String], writeSupport: ParquetWriteSupport[T]) =
    new TypedFixedPathParquetTuple[T](paths, null, writeSupport)
}

/**
 * Typed Parquet tuple source/sink.
 */
trait TypedParquet[T] extends FileSource with Mappable[T]
  with TypedSink[T] with HasFilterPredicate {

  val readSupport: ParquetReadSupport[T]
  val writeSupport: ParquetWriteSupport[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

  override def hdfsScheme = {
    val scheme = new TypedParquetTupleScheme[T](readSupport, writeSupport, withFilter)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

class TypedFixedPathParquetTuple[T](val paths: Seq[String], val readSupport: ParquetReadSupport[T],
  val writeSupport: ParquetWriteSupport[T]) extends FixedPathSource(paths: _*) with TypedParquet[T]
