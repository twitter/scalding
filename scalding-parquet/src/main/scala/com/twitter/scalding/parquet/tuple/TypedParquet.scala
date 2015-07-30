package com.twitter.scalding.parquet.tuple

import org.apache.parquet.filter2.predicate.FilterPredicate
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
   *  val parquetTuple = TypedParquet[SampleClass](Seq(outputPath))
   *
   * @param paths paths of parquet I/O
   * @tparam T Tuple type
   * @return a typed parquet source.
   */
  def apply[T](paths: Seq[String])(implicit readSupport: ParquetReadSupport[T]): TypedParquet[T] =
    new TypedFixedPathParquetTuple[T](paths, readSupport, null)

  def apply[T](path: String)(implicit readSupport: ParquetReadSupport[T]): TypedParquet[T] = apply[T](Seq(path))

  /**
   * Create readable typed parquet source with filter predicate.
   */
  def apply[T](paths: Seq[String], fp: FilterPredicate)(implicit readSupport: ParquetReadSupport[T]): TypedParquet[T] =
    new TypedFixedPathParquetTuple[T](paths, readSupport, null) {
      override def withFilter = Some(fp)
    }

  def apply[T](path: String, fp: FilterPredicate)(implicit readSupport: ParquetReadSupport[T]): TypedParquet[T] =
    apply[T](Seq(path), fp)
}

object TypedParquetSink {
  /**
   * Create typed parquet sink.
   * Here is an example:
   *  import com.twitter.scalding.parquet.tuple.macros.Macros._
   *  val sink = TypedParquetSink[SampleClass](Seq(outputPath))
   *
   * @param paths paths of parquet I/O
   * @tparam T Tuple type
   * @return a typed parquet source.
   */
  def apply[T](paths: Seq[String])(implicit writeSupport: ParquetWriteSupport[T]): TypedParquet[T] =
    new TypedFixedPathParquetTuple[T](paths, null, writeSupport)

  def apply[T](path: String)(implicit writeSupport: ParquetWriteSupport[T]): TypedParquet[T] = apply[T](Seq(path))
}

/**
 * Typed Parquet tuple source/sink.
 */
trait TypedParquet[T] extends FileSource with Mappable[T]
  with TypedSink[T] with HasFilterPredicate {

  def readSupport: ParquetReadSupport[T]
  def writeSupport: ParquetWriteSupport[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

  override def hdfsScheme = {
    val scheme = new TypedParquetTupleScheme[T](readSupport, writeSupport, withFilter)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

class TypedFixedPathParquetTuple[T](val paths: Seq[String], val readSupport: ParquetReadSupport[T],
  val writeSupport: ParquetWriteSupport[T]) extends FixedPathSource(paths: _*) with TypedParquet[T]
