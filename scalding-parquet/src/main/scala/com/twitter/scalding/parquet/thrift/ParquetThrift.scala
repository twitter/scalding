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
import _root_.parquet.filter2.predicate.FilterPredicate
import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.{ ColumnProjectionGlob, HasColumnProjection, HasFilterPredicate }
import com.twitter.scalding.source.{ DailySuffixSource, HourlySuffixSource }
import java.io.Serializable
import org.apache.thrift.{ TBase, TFieldIdEnum }

object ParquetThrift extends Serializable {
  type ThriftBase = TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]
}

trait ParquetThrift[This <: ParquetThrift[This, T], T <: ParquetThrift.ThriftBase] extends FileSource
  with SingleMappable[T] with TypedSink[T] with LocalTapSource with HasFilterPredicate[This] with HasColumnProjection[This] {

  def mf: Manifest[T]

  override def hdfsScheme = {

    val config = new ParquetValueScheme.Config[T].withRecordClass(mf.erasure.asInstanceOf[Class[T]])

    val configWithFp = filterPredicate match {
      case Some(fp) => config.withFilterPredicate(fp)
      case None => config
    }

    val configWithProjection = globsInParquetStringFormat match {
      case Some(s) => configWithFp.withProjectionString(s)
      case None => configWithFp
    }

    val scheme = new ParquetTBaseScheme[T](configWithProjection)

    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

}

final class DailySuffixParquetThrift[T <: ParquetThrift.ThriftBase](
  path: String,
  dateRange: DateRange,
  override val filterPredicate: Option[FilterPredicate] = None,
  override val columnGlobs: Set[ColumnProjectionGlob] = Set())(implicit override val mf: Manifest[T])
  extends DailySuffixSource(path, dateRange) with ParquetThrift[DailySuffixParquetThrift[T], T] {

  override protected def copyWithFilter(fp: FilterPredicate): DailySuffixParquetThrift[T] =
    new DailySuffixParquetThrift[T](path, dateRange, Some(fp), columnGlobs)

  override protected def copyWithColumnGlobs(globs: Set[ColumnProjectionGlob]): DailySuffixParquetThrift[T] =
    new DailySuffixParquetThrift[T](path, dateRange, filterPredicate, globs)
}

final class HourlySuffixParquetThrift[T <: ParquetThrift.ThriftBase](
  path: String,
  dateRange: DateRange,
  override val filterPredicate: Option[FilterPredicate] = None,
  override val columnGlobs: Set[ColumnProjectionGlob] = Set())(implicit override val mf: Manifest[T])
  extends HourlySuffixSource(path, dateRange) with ParquetThrift[HourlySuffixParquetThrift[T], T] {

  override protected def copyWithFilter(fp: FilterPredicate): HourlySuffixParquetThrift[T] =
    new HourlySuffixParquetThrift[T](path, dateRange, Some(fp), columnGlobs)

  override protected def copyWithColumnGlobs(globs: Set[ColumnProjectionGlob]): HourlySuffixParquetThrift[T] =
    new HourlySuffixParquetThrift[T](path, dateRange, filterPredicate, globs)
}

final class FixedPathParquetThrift[T <: ParquetThrift.ThriftBase] private (
  paths: Seq[String],
  override val filterPredicate: Option[FilterPredicate] = None,
  override val columnGlobs: Set[ColumnProjectionGlob] = Set())(implicit override val mf: Manifest[T])
  extends FixedPathSource(paths: _*) with ParquetThrift[FixedPathParquetThrift[T], T] {

  def this(paths: String*)(implicit mf: Manifest[T]) = this(paths)

  override protected def copyWithFilter(fp: FilterPredicate): FixedPathParquetThrift[T] =
    new FixedPathParquetThrift[T](paths, Some(fp), columnGlobs)

  override protected def copyWithColumnGlobs(globs: Set[ColumnProjectionGlob]): FixedPathParquetThrift[T] =
    new FixedPathParquetThrift[T](paths, filterPredicate, globs)
}