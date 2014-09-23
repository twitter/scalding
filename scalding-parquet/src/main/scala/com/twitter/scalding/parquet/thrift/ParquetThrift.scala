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

package com.twitter.scalding.parquet.thrift

import _root_.parquet.cascading.{ ParquetTBaseScheme, ParquetValueScheme }
import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.{ HasColumnProjection, HasFilterPredicate }
import com.twitter.scalding.source.{ DailySuffixSource, HourlySuffixSource }
import java.io.Serializable
import org.apache.thrift.{ TBase, TFieldIdEnum }

object ParquetThrift extends Serializable {
  type ThriftBase = TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]
}

trait ParquetThriftBase[T] extends FileSource with SingleMappable[T] with TypedSink[T] with LocalTapSource with HasFilterPredicate with HasColumnProjection {

  def mf: Manifest[T]

  def config: ParquetValueScheme.Config[T] = {

    val config = new ParquetValueScheme.Config[T].withRecordClass(mf.erasure.asInstanceOf[Class[T]])

    val configWithFp = withFilter match {
      case Some(fp) => config.withFilterPredicate(fp)
      case None => config
    }

    val configWithProjection = globsInParquetStringFormat match {
      case Some(s) => configWithFp.withProjectionString(s)
      case None => configWithFp
    }

    // TODO: remove asInstanceOf after the fix ships in parqet-mr
    configWithProjection.asInstanceOf[ParquetValueScheme.Config[T]]
  }

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

}

trait ParquetThrift[T <: ParquetThrift.ThriftBase] extends ParquetThriftBase[T] {

  override def hdfsScheme = {
    val scheme = new ParquetTBaseScheme[T](this.config)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }

}

/**
 * When Using these sources or creating subclasses of them, you can
 * provide a filter predicate and / or a set of fields (columns) to keep (project).
 *
 * The filter predicate will be pushed down to the input format, potentially
 * making the filter significantly more efficient than a filter applied to
 * a TypedPipe (parquet push-down filters can skip reading entire chunks of data off disk).
 *
 * For data with a large schema (many fields / columns), providing the set of columns
 * you intend to use can also make your job significantly more efficient (parquet column projection
 * push-down will skip reading unused columns from disk).
 * The columns are specified in the format described here:
 * https://github.com/apache/incubator-parquet-mr/blob/master/parquet_cascading.md#21-projection-pushdown-with-thriftscrooge-records
 *
 * These settings are defined in the traits [[com.twitter.scalding.parquet.HasFilterPredicate]]
 * and [[com.twitter.scalding.parquet.HasColumnProjection]]
 *
 * Here are two ways you can use these in a parquet source:
 *
 * {{{
 * class MyParquetSource(dr: DateRange) extends DailySuffixParquetThrift("/a/path", dr)
 *
 * val mySourceFilteredAndProjected = new MyParquetSource(dr) {
 *   override val withFilter: Option[FilterPredicate] = Some(myFp)
 *   override val withColumns: Set[String] = Set("a/b/c", "x/y")
 * }
 * }}}
 *
 * The other way is to add these as constructor arguments:
 *
 * {{{
 * class MyParquetSource(
 *   dr: DateRange,
 *   override val withFilter: Option[FilterPredicate] = None
 *   override val withColumns: Set[String] = Set()
 * ) extends DailySuffixParquetThrift("/a/path", dr)
 *
 * val mySourceFilteredAndProjected = new MyParquetSource(dr, Some(myFp), Set("a/b/c", "x/y"))
 * }}}
 */
class DailySuffixParquetThrift[T <: ParquetThrift.ThriftBase](
  path: String,
  dateRange: DateRange)(implicit override val mf: Manifest[T])
  extends DailySuffixSource(path, dateRange) with ParquetThrift[T]

class HourlySuffixParquetThrift[T <: ParquetThrift.ThriftBase](
  path: String,
  dateRange: DateRange)(implicit override val mf: Manifest[T])
  extends HourlySuffixSource(path, dateRange) with ParquetThrift[T]

class FixedPathParquetThrift[T <: ParquetThrift.ThriftBase](paths: String*)(implicit override val mf: Manifest[T])
  extends FixedPathSource(paths: _*) with ParquetThrift[T]