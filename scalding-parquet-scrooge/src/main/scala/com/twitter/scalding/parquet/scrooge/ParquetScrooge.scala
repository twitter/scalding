package com.twitter.scalding.parquet.scrooge

import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.thrift.ParquetThriftBaseFileSource
import com.twitter.scalding.source.{ DailySuffixSource, HourlySuffixSource }
import com.twitter.scrooge.ThriftStruct

import scala.reflect.ClassTag

trait ParquetScrooge[T <: ThriftStruct] extends ParquetThriftBaseFileSource[T] {

  override def hdfsScheme = {
    // See docs in Parquet346ScroogeScheme
    val scheme = new Parquet346ScroogeScheme[T](this.config)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }

}

class DailySuffixParquetScrooge[T <: ThriftStruct](
  path: String,
  dateRange: DateRange)(implicit override val ct: ClassTag[T])
  extends DailySuffixSource(path, dateRange) with ParquetScrooge[T]

class HourlySuffixParquetScrooge[T <: ThriftStruct](
  path: String,
  dateRange: DateRange)(implicit override val ct: ClassTag[T])
  extends HourlySuffixSource(path, dateRange) with ParquetScrooge[T]

class FixedPathParquetScrooge[T <: ThriftStruct](paths: String*)(implicit override val ct: ClassTag[T])
  extends FixedPathSource(paths: _*) with ParquetScrooge[T]
