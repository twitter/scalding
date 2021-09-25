package com.twitter.scalding.parquet.thrift

import cascading.scheme.Scheme
import com.twitter.scalding.typed.{ PartitionSchemed, PartitionUtil }
import com.twitter.scalding.{ FixedPathSource, HadoopSchemeInstance, TupleConverter, TupleSetter }

import scala.reflect.ClassTag

/**
 * Scalding source to read or write partitioned Parquet thrift data.
 *
 * For writing it expects a pair of `(P, T)`, where `P` is the data used for partitioning and
 * `T` is the thrift object. `P` must be either a String or a tuple of Strings.
 * Below is an example.
 * {{{
 * val data: TypedPipe[MyThriftObject] = ???
 * data.map{ obj =>
 *   ( (obj.country, obj.city), obj)
 * }.write(PartitionedParquetThriftSource[(String, String), MyThriftObject](path, "%s/%s"))
 * }}}
 *
 * For reading it produces a pair `(P, T)` where `P` is the partition data, `T` is the corresponding
 * thrift object. Below is an example.
 * {{{
 * val in: TypedPipe[(String, String), MyThriftObject] =
 * TypedPipe.from( PartitionedParquetThriftSource[(String, String), MyThriftObject](path, "%s/%s") )
 * }}}
 *
 */
case class PartitionedParquetThriftSource[P, T <: ParquetThrift.ThriftBase](path: String, template: String)(implicit val ct: ClassTag[T],
  val valueSetter: TupleSetter[T], val valueConverter: TupleConverter[T], val partitionSetter: TupleSetter[P], val partitionConverter: TupleConverter[P])
  extends FixedPathSource(path) with ParquetThriftBase[T] with PartitionSchemed[P, T] with Serializable {

  override val fields = PartitionUtil.toFields(0, implicitly[TupleSetter[T]].arity)

  assert(
    fields.size == valueSetter.arity,
    "The number of fields needs to be the same as the arity of the value setter")

  // Create the underlying scheme and explicitly set the source, sink fields to be only the specified fields
  override def hdfsScheme = {
    // See docs in Parquet346TBaseScheme
    val baseScheme = new Parquet346TBaseScheme[T](this.config)
    val scheme = HadoopSchemeInstance(baseScheme.asInstanceOf[Scheme[_, _, _, _, _]])
    scheme.setSinkFields(fields)
    scheme.setSourceFields(fields)
    scheme
  }
}
