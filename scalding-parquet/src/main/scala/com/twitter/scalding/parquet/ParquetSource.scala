package com.twitter.scalding.parquet

import cascading.tuple.Fields
import com.twitter.scalding.{TupleConverter, Mappable, HadoopSchemeInstance, FixedPathSource}
import parquet.cascading.{ParquetTBaseScheme, ParquetTupleScheme}
import parquet.org.apache.thrift.{TFieldIdEnum, TBase}
import cascading.scheme.Scheme

object ParquetSource {
  def apply(fields: Fields, path: String) =
    new ParquetSource(fields, Seq(path))

  def apply(fields: Fields, paths: Seq[String]) =
    new ParquetSource(fields, paths)
}

class ParquetSource(fields: Fields, paths: Seq[String]) extends FixedPathSource(paths: _*) {
  override def hdfsScheme = HadoopSchemeInstance(new ParquetTupleScheme(fields).asInstanceOf[Scheme[_,_,_,_,_]])
}

/*
case class ParquetTBaseSource[T](p : String)(implicit m : Manifest[T])
  extends FixedPathSource(p)
  with Mappable[T]
  with java.io.Serializable {

  val klass = m.erasure.asSubclass[TBase[_, _ <: TFieldIdEnum]](classOf[TBase[_, _ <: TFieldIdEnum]])

  override def hdfsScheme = HadoopSchemeInstance(new ParquetTBaseScheme(klass).asInstanceOf[Scheme[_,_,_,_,_]])
  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](implicitly[TupleConverter[T]])
}*/
