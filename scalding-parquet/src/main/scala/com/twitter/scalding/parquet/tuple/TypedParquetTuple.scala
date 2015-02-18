package com.twitter.scalding.parquet.tuple

import _root_.parquet.cascading.ParquetTupleScheme
import _root_.parquet.schema.MessageType
import cascading.scheme.Scheme
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.parquet.HasFilterPredicate

/**
 * Typed parquet tuple source/sink, if used as sink, user should provide parquet schema definition.
 * @author Jian Tang
 */
object TypedParquetTuple {

  def apply[T: Manifest: TupleConverter: TupleSetter](paths: Seq[String])(implicit sinkFields: Fields, messageType: MessageType) =
    new TypedFixedPathParquetTuple[T](paths, Fields.ALL, sinkFields, Some(messageType.toString))

  def apply[T: Manifest: TupleConverter: TupleSetter](paths: Seq[String], sourceFields: Fields) =
    new TypedFixedPathParquetTuple[T](paths, sourceFields)
}

/**
 * Typed Parquet tuple source/sink.
 */
trait TypedParquetTuple[T] extends FileSource with Mappable[T] with TypedSink[T] with HasFilterPredicate {

  implicit val tupleConverter: TupleConverter[T]

  implicit val tupleSetter: TupleSetter[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](tupleConverter)

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](tupleSetter)

  /**
   * Parquet schema definition for mapping with type T.
   */
  def parquetSchema: Option[String]

  override def hdfsScheme = {
    val scheme = parquetSchema match {
      case Some(messageType) => new ParquetTupleScheme(sourceFields, sinkFields, messageType)
      case _ =>
        withFilter match {
          case Some(filterPredicate) => new ParquetTupleScheme(filterPredicate, sourceFields)
          case _ => new ParquetTupleScheme(sourceFields)
        }
    }
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

class TypedFixedPathParquetTuple[T](paths: Seq[String],
  override val sourceFields: Fields = Fields.ALL,
  override val sinkFields: Fields = Fields.UNKNOWN,
  override val parquetSchema: Option[String] = None)(implicit override val tupleConverter: TupleConverter[T],
    override val tupleSetter: TupleSetter[T]) extends FixedPathSource(paths: _*) with TypedParquetTuple[T]
