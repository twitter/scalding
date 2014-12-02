package com.twitter.scalding.parquet.scrooge

import _root_.parquet.scrooge.ParquetScroogeScheme
import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.thrift.ParquetThriftBase
import com.twitter.scalding.source.{ DailySuffixSource, HourlySuffixSource }
import com.twitter.scrooge.ThriftStruct

trait ParquetScrooge[T <: ThriftStruct] extends ParquetThriftBase[T] {

  override def hdfsScheme = {
    val scheme = new ParquetScroogeScheme[T](this.config)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }

}

class DailySuffixParquetScrooge[T <: ThriftStruct](
  path: String,
  dateRange: DateRange)(implicit override val mf: Manifest[T])
  extends DailySuffixSource(path, dateRange) with ParquetScrooge[T]

class HourlySuffixParquetScrooge[T <: ThriftStruct](
  path: String,
  dateRange: DateRange)(implicit override val mf: Manifest[T])
  extends HourlySuffixSource(path, dateRange) with ParquetScrooge[T]

class FixedPathParquetScrooge[T <: ThriftStruct](paths: String*)(implicit override val mf: Manifest[T])
  extends FixedPathSource(paths: _*) with ParquetScrooge[T]
