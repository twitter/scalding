package com.twitter.scalding.parquet.scrooge

import _root_.cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.thrift.ParquetThriftBase
import com.twitter.scalding.typed.{ PartitionSchemed, PartitionUtil }
import com.twitter.scrooge.ThriftStruct

import scala.reflect.ClassTag

/**
 * Scalding source to read or write partitioned Parquet scrooge data.
 *
 * For writing it expects a pair of `(P, T)`, where `P` is the data used for partitioning and
 * `T` is the scrooge object. `P` must be either a String or a tuple of Strings.
 * Below is an example.
 * {{{
 * val data: TypedPipe[MyScroogeObject] = ???
 * data.map { obj =>
 *   ( (obj.country, obj.city), obj)
 * }.write(PartitionedParquetScroogeSource[(String, String), MyScroogeObject](path, "%s/%s"))
 * }}}
 *
 * For reading it produces a pair `(P, T)` where `P` is the partition data, `T` is the corresponding
 * scrooge object. Below is an example.
 * {{{
 * val in: TypedPipe[(String, String), MyScroogeObject] =
 * TypedPipe.from( PartitionedParquetScroogeSource[(String, String), MyScroogeObject](path, "%s/%s") )
 * }}}
 *
 */
case class PartitionedParquetScroogeSource[P, T <: ThriftStruct](path: String, template: String)(implicit val ct: ClassTag[T],
  val valueSetter: TupleSetter[T], val valueConverter: TupleConverter[T], val partitionSetter: TupleSetter[P], val partitionConverter: TupleConverter[P])
  extends FixedPathSource(path) with ParquetThriftBase[T] with PartitionSchemed[P, T] with Serializable {

  override val fields = PartitionUtil.toFields(0, implicitly[TupleSetter[T]].arity)

  assert(
    fields.size == valueSetter.arity,
    "The number of fields needs to be the same as the arity of the value setter")

  // Create the underlying scheme and explicitly set the source, sink fields to be only the specified fields
  override def hdfsScheme = {
    val scroogeScheme = new Parquet346ScroogeScheme[T](this.config)
    val scheme = HadoopSchemeInstance(scroogeScheme.asInstanceOf[Scheme[_, _, _, _, _]])
    scheme.setSinkFields(fields)
    scheme.setSourceFields(fields)
    scheme
  }
}
