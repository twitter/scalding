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

import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.{
  StrictColumnProjectionString,
  DeprecatedColumnProjectionString,
  HasColumnProjection,
  HasFilterPredicate,
  ParquetValueScheme
}
import com.twitter.scalding.source.{ DailySuffixSource, HourlySuffixSource }
import java.io.Serializable
import org.apache.thrift.{ TBase, TFieldIdEnum }

import scala.reflect.ClassTag
import com.twitter.scalding.quotation.Projections
import com.twitter.scalding.typed.ProjectionMeta
import org.apache.hadoop.mapred.JobConf
import org.apache.parquet.hadoop.thrift.ThriftReadSupport
import com.twitter.scalding.quotation.TypeReference
import com.twitter.scalding.quotation.Projection
import com.twitter.scalding.quotation.Property
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

object ParquetThrift extends Serializable {
  type ThriftBase = TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]

  private val log = LoggerFactory.getLogger(this.getClass)
  
  def projectionMeta[T: ClassTag](superClass: Class[_]) = {
    def setConf(c: Configuration, p: Projections) = {

      def snakeCase(s: String) =
        s.flatMap {
          case c if c.isUpper => s"_${c.toLower}"
          case c => s"$c"
        }
        
      def toString(p: Projection): String =
        p match {
          case TypeReference(tpe) => ""
          case Property(path: TypeReference, name, tpe) => snakeCase(name)
          case Property(path, name, tpe) => s"${toString(path)}.${snakeCase(name)}"
        }

      val projectionString = p.set.map(toString).mkString(";")
      log.info(s"Automatic projection pushdown: $projectionString")
      ThriftReadSupport.setStrictFieldProjectionFilter(c, projectionString);
    }
    ProjectionMeta(implicitly[ClassTag[T]], superClass, setConf)
  }
}

trait ParquetThriftBase[T] extends LocalTapSource with HasFilterPredicate with HasColumnProjection {

  implicit def ct: ClassTag[T]

  def config: ParquetValueScheme.Config[T] = {
    val clazz = ct.runtimeClass.asInstanceOf[Class[T]]
    val config = new ParquetValueScheme.Config[T].withRecordClass(clazz)
    val configWithFp = withFilter match {
      case Some(fp) => config.withFilterPredicate(fp)
      case None => config
    }

    val configWithProjection = columnProjectionString match {
      case Some(s @ DeprecatedColumnProjectionString(_)) => configWithFp.withProjectionString(s.asSemicolonString)
      case Some(s @ StrictColumnProjectionString(_)) => configWithFp.withStrictProjectionString(s.asSemicolonString)
      case None => configWithFp
    }

    configWithProjection
  }
}

trait ParquetThriftBaseFileSource[T] extends FileSource with ParquetThriftBase[T] with SingleMappable[T] with TypedSink[T] {
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def projectionMeta = Some(ParquetThrift.projectionMeta[T](classOf[TBase[_, _]]))
}

trait ParquetThrift[T <: ParquetThrift.ThriftBase] extends ParquetThriftBaseFileSource[T] {

  override def hdfsScheme = {
    // See docs in Parquet346TBaseScheme
    val scheme = new Parquet346TBaseScheme[T](this.config)
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
 * https://github.com/apache/parquet-mr/blob/master/parquet_cascading.md#21-projection-pushdown-with-thriftscrooge-records
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
 *   override val withColumnProjections: Set[String] = Set("a.b.c", "x.y")
 * }
 * }}}
 *
 * The other way is to add these as constructor arguments:
 *
 * {{{
 * class MyParquetSource(
 *   dr: DateRange,
 *   override val withFilter: Option[FilterPredicate] = None
 *   override val withColumnProjections: Set[String] = Set()
 * ) extends DailySuffixParquetThrift("/a/path", dr)
 *
 * val mySourceFilteredAndProjected = new MyParquetSource(dr, Some(myFp), Set("a.b.c", "x.y"))
 * }}}
 */
class DailySuffixParquetThrift[T <: ParquetThrift.ThriftBase](
  path: String,
  dateRange: DateRange)(implicit override val ct: ClassTag[T])
  extends DailySuffixSource(path, dateRange) with ParquetThrift[T]

class HourlySuffixParquetThrift[T <: ParquetThrift.ThriftBase](
  path: String,
  dateRange: DateRange)(implicit override val ct: ClassTag[T])
  extends HourlySuffixSource(path, dateRange) with ParquetThrift[T]

class FixedPathParquetThrift[T <: ParquetThrift.ThriftBase](paths: String*)(implicit override val ct: ClassTag[T])
  extends FixedPathSource(paths: _*) with ParquetThrift[T]
