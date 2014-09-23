/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.parquet.tuple

import _root_.parquet.cascading.ParquetTupleScheme
import cascading.scheme.Scheme
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.parquet.HasFilterPredicate
import com.twitter.scalding.source.{ DailySuffixSource, HourlySuffixSource }

object ParquetTupleSource {
  def apply(fields: Fields, paths: String*) = new FixedPathParquetTuple(fields, paths: _*)
}

/**
 * User should define their own source like:
 * class MySource(path: String, dateRange: DateRange, requestedFields: Fields) extends DailySuffixParquetTuple(path, dateRange, requestedFields) with Mappable2[Int, Int] with TypedSink2[Int,Int]
 */
trait ParquetTupleSource extends FileSource with HasFilterPredicate {
  def fields: Fields

  override def hdfsScheme = {

    val scheme = withFilter match {
      case Some(fp) => new ParquetTupleScheme(fp, fields)
      case None => new ParquetTupleScheme(fields)
    }

    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }

}

/**
 * See [[com.twitter.scalding.parquet.thrift.DailySuffixParquetThrift]] for documentation on
 * how to specify filter predicates for these sources.
 */
class DailySuffixParquetTuple(
  path: String,
  dateRange: DateRange,
  override val fields: Fields) extends DailySuffixSource(path, dateRange) with ParquetTupleSource

class HourlySuffixParquetTuple(
  path: String,
  dateRange: DateRange,
  override val fields: Fields) extends HourlySuffixSource(path, dateRange) with ParquetTupleSource

class FixedPathParquetTuple(
  override val fields: Fields,
  paths: String*) extends FixedPathSource(paths: _*) with ParquetTupleSource